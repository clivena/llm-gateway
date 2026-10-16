# -*- coding: utf-8 -*-
"""
llm-gateway token 估算校准 —— 让配额预扣从"猜"变成"算"

它回答一个问题：**我们的 token 估算和上游真实用量差多少？**

为什么值得单独一个脚本：配额是唯一直接对应真金白银的组件，而它的预扣额来自估算。
旧实现用 `length / 4`（英文经验公式），真机实测 28 个汉字被算成 7 tokens，
上游回传的真实 prompt_tokens 是 45 —— 比值 6.43×。这意味着一份 20 万 token 的配额，
中文场景实际可消耗约 130 万。**偏差随语言变化，是系统性低估，不是"一点点误差"**。

做法（全部实测，不假设任何系数）：
  1) 启动服务，从 /admin/pricing 读**当前生效**的估算系数
     —— 不硬编码副本，配置一改脚本就跟着变，不会悄悄测错
  2) 发多组探针（纯中文短/长、纯英文短/长、中英混合、多消息），
     每次请求都带上游回传的真实 usage.prompt_tokens
  3) 用当前系数预测每组，算 预测 / 真实 的比值
  4) 用两组不同长度的纯中文探针反推 cjk-per-char、再由纯英文探针反推 ascii-per-char，
     打印建议配置（这是"系数的正当来源"）
  5) 断言所有探针的比值落在容差带内

两个刻意的设计：
  · **用一次性租户名**（带时间戳）。缓存 namespace 是 <租户>:<模型>，
    换租户等于换 namespace，探针必然 MISS —— 否则第二次跑脚本时命中缓存，
    usage 会是 0/0，校准直接失去依据。
  · **反复强调"比值"而不是绝对值**。token 数由上游分词器决定，
    脚本不该假装知道绝对值应该等于什么，只判断"估算离真实有多远"。

用法::

    docker compose up -d redis postgres
    python scripts/calibrate-token-estimator.py

可覆盖的环境变量：GATEWAY_DIR / CALIB_LOG / GATEWAY_PORT / CALIB_MODEL /
CALIB_TOLERANCE / CALIB_MVN_CMD / CALIB_MAVEN_REPO / CALIB_SAFETY_FACTOR / JAVA_HOME。
"""
import collections
import http.client
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------- 路径与配置
ROOT = Path(os.environ.get("GATEWAY_DIR") or Path(__file__).resolve().parents[1])
LOG = Path(os.environ.get("CALIB_LOG") or (ROOT / "out" / "calibrate-token-estimator.log"))
PORT = int(os.environ.get("GATEWAY_PORT") or 8080)
MODEL = os.environ.get("CALIB_MODEL") or "deepseek-flash"

# 容差带：估算 / 真实。0.85~1.15 意味着"估算在真实值的 ±15% 内"。
# 不设成 ±5% 是因为上游分词器对同一段文本的行为本身就有波动（不同
# 模型版本、不同模板），把容差拧到过紧会让脚本变成噪音源。
TOLERANCE = float(os.environ.get("CALIB_TOLERANCE") or 0.15)

RUN_TAG = time.strftime("%H%M%S")
# 每次运行一个新租户名 → 新缓存 namespace → 探针必然 MISS 缓存。
TENANT = "token-calib-%s" % RUN_TAG

lines = []


def log(msg=""):
    text = str(msg)
    lines.append(text)
    try:
        print(text, flush=True)
    except Exception:
        pass


def flush():
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG, "w", encoding="utf-8", errors="replace") as f:
        f.write("\n".join(lines))


def _pick_mvn() -> str:
    if os.environ.get("CALIB_MVN_CMD"):
        return os.environ["CALIB_MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo() -> str:
    if os.environ.get("CALIB_MAVEN_REPO"):
        return os.environ["CALIB_MAVEN_REPO"]
    local = Path.home() / ".workbuddy/binaries/maven/repo"
    return str(local) if local.exists() else str(Path.home() / ".m2" / "repository")


MVN = _pick_mvn()
REPO = _pick_repo()
JAVA_HOME = os.environ.get("JAVA_HOME", "")

# ---------------------------------------------------------------- 探针
# 纯中文 / 纯英文各准备长短两组：反推系数需要"同类型、不同长度"的两点
# （一点只能解出一个未知量，解不出截距）。
PROBES = [
    ("中文-短", [{"role": "user", "content": "请解释缓存"}]),
    ("中文-长", [{"role": "user", "content":
                  "请用通俗的语言解释什么是语义缓存，以及它和精确缓存之间的区别，"
                  "尽量举一个具体的例子说明，并指出各自的适用场景和可能的坑"}]),
    ("英文-短", [{"role": "user", "content": "Explain caching"}]),
    ("英文-长", [{"role": "user", "content":
                  "Please explain in plain language what a semantic cache is, "
                  "how it differs from an exact cache, and give one concrete example "
                  "with the tradeoffs involved"}]),
    ("混合", [{"role": "user", "content": "请解释 semantic cache 与 exact cache 的区别"}]),
    ("多消息", [
        {"role": "system", "content": "你是一个有帮助的助手，请用中文回答，尽量简短。"},
        {"role": "user", "content": "什么是语义缓存？"},
        {"role": "assistant", "content": "语义缓存按问题含义检索历史答案。"},
        {"role": "user", "content": "那它和精确缓存比有什么代价？"},
    ]),
]

# ---------------------------------------------------------------- 估算模型
# 与 Java 侧 TokenEstimator 必须完全一致。两边不一致的话，
# 脚本测的就不是线上行为 —— 所以这里把区间与公式原样搬过来，并注明出处。
CJK_RANGES = [
    (0x4E00, 0x9FFF),   # CJK 统一表意文字
    (0x3400, 0x4DBF),   # 扩展 A
    (0xF900, 0xFAFF),   # 兼容表意文字
    (0x3000, 0x303F),   # CJK 标点（、。「」等）
    (0xFF00, 0xFFEF),   # 全角形式（，！？ＡＢ等）
    (0x3040, 0x30FF),   # 平假名 / 片假名
    (0xAC00, 0xD7AF),   # 谚文
]


def is_cjk(cp: int) -> bool:
    return any(lo <= cp <= hi for lo, hi in CJK_RANGES)


def count_chars(text: str):
    """返回 (cjk 字符数, 非 cjk 字符数)。按 codePoint 计数，与 Java 侧一致。"""
    cjk = sum(1 for ch in text if is_cjk(ord(ch)))
    return cjk, len(text) - cjk


def weighted(text: str, coeff) -> float:
    cjk, other = count_chars(text)
    return cjk * coeff["cjkPerChar"] + other * coeff["asciiPerChar"]


def predict(messages, coeff) -> int:
    """复现 Java 侧 estimatePrompt：一次性开销 + 每条开销 + 内容与 role 的加权。"""
    total = float(coeff.get("baseOverhead") or 0.0)
    for m in messages:
        total += coeff["perMessageOverhead"]
        total += weighted(m.get("content") or "", coeff)
        total += weighted(m.get("role") or "", coeff)
    return max(1, math.ceil(total * coeff["safetyFactor"]))


def legacy_estimate(messages) -> int:
    """旧实现（本脚本要量化它有多差）：最后一条 user 消息的长度 / 4。"""
    question = ""
    for m in messages:
        if m.get("role") == "user":
            question = m.get("content") or ""
    return max(1, len(question) // 4)


# ---------------------------------------------------------------- HTTP
def tcp_ok(timeout=2):
    import socket
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect(("127.0.0.1", PORT))
        return True
    except Exception:
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def api_get(path, timeout=10):
    try:
        r = NO_PROXY_OPENER.open("http://127.0.0.1:%d%s" % (PORT, path), timeout=timeout)
        return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as he:
        try:
            return he.code, json.loads(he.read().decode("utf-8", "replace"))
        except Exception:
            return he.code, None
    except Exception as e:
        return -1, {"error": repr(e)}


def post_chat(messages, timeout=120):
    """发一次非流式请求，返回 (状态码, usage dict 或 None, 原始响应体)。"""
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    payload = {
        "model": MODEL,
        "stream": False,
        "temperature": 0.0,
        # max_tokens 压到最小：本次只关心 prompt_tokens，
        # 让输出尽量短 = 尽量少花钱。它不影响 prompt 侧计数。
        "max_tokens": 8,
        "feature": "token-calibration",
        "messages": messages,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    try:
        conn.request("POST", "/v1/chat/completions", body=body, headers={
            "Content-Type": "application/json",
            "X-Api-Key": TENANT,
        })
        resp = conn.getresponse()
        raw = resp.read()
        status = resp.status
    finally:
        try:
            conn.close()
        except Exception:
            pass
    try:
        data = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        data = None
    return status, (data or {}).get("usage"), data


# ---------------------------------------------------------------- 服务生命周期
def start_app():
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"

    # 宿主注入的 SERVER__PORT 会被宽松绑定成 server.port，把服务起在别人的端口上。
    hijacked = {k: v for k, v in env.items()
                if k in ("SERVER__PORT", "SERVER_PORT", "SPRING_APPLICATION_JSON")}
    for k in hijacked:
        env.pop(k, None)

    cmd = [
        MVN, "-B", "-Duser.language=en",
        "-Dmaven.repo.local=" + REPO,
        "-f", os.path.join(ROOT, "pom.xml"),
        "spring-boot:run",
        "-Dspring-boot.run.profiles=local",
        "-Dspring-boot.run.arguments=--server.port=%d" % PORT,
    ]
    log()
    log("=" * 72)
    log("Step 1 · 启动服务（端口 %d，模型 %s）" % (PORT, MODEL))
    log("=" * 72)
    if hijacked:
        log("  [修复] 检测到端口劫持环境变量，已从子进程摘除: %s" % hijacked)

    proc = subprocess.Popen(
        cmd, cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        bufsize=1, universal_newlines=False,
    )
    tail = collections.deque(maxlen=400)

    def reader():
        for raw in iter(proc.stdout.readline, b""):
            tail.append(raw.decode("utf-8", errors="replace").rstrip())

    threading.Thread(target=reader, daemon=True).start()
    return proc, tail


def wait_ready(tail, timeout=300):
    """就绪判定用 TCP 直连，不看 /actuator/health（Redis/Postgres 之一未就绪时它会 503）。"""
    t0 = time.time()
    log("  等待 127.0.0.1:%d 开始监听（最长 %ds）..." % (PORT, timeout))
    while time.time() < t0 + timeout:
        if any("APPLICATION FAILED TO START" in l or "BUILD FAILURE" in l for l in tail):
            return False
        if tcp_ok():
            log("  端口已监听（启动耗时约 %.0fs）" % (time.time() - t0))
            return True
        time.sleep(2)
    return False


# ---------------------------------------------------------------- 3. 采集
def collect(coeff):
    log()
    log("=" * 72)
    log("Step 2 · 采集真实用量（租户 %s，每次运行独立 namespace 以避开缓存）" % TENANT)
    log("=" * 72)
    log("  当前生效系数: cjk=%s ascii=%s overhead=%s safety=%s"
        % (coeff["cjkPerChar"], coeff["asciiPerChar"],
           coeff["perMessageOverhead"], coeff["safetyFactor"]))

    samples = []
    for name, messages in PROBES:
        try:
            status, usage, body = post_chat(messages)
        except Exception as e:
            log("  [%s] 请求失败: %r" % (name, e))
            continue
        if status != 200 or not usage:
            log("  [%s] HTTP %s 或响应无 usage，跳过（body=%s）"
                % (name, status, json.dumps(body, ensure_ascii=False)[:160] if body else None))
            continue

        actual = usage.get("prompt_tokens")
        predicted = predict(messages, coeff)
        legacy = legacy_estimate(messages)
        total_cjk, total_other = 0, 0
        for m in messages:
            c, o = count_chars(m.get("content") or "")
            rc, ro = count_chars(m.get("role") or "")
            total_cjk += c + rc
            total_other += o + ro

        ratio = (predicted / actual) if actual else None
        samples.append({
            "name": name, "messages": messages, "actual": actual,
            "predicted": predicted, "legacy": legacy, "ratio": ratio,
            "cjk": total_cjk, "other": total_other,
        })
        log("  [%-6s] 汉字%-4d 其他%-4d 消息%d  真实=%-5s 估算=%-5s 比值=%s  旧公式=%-4s(差 %sx)"
            % (name, total_cjk, total_other, len(messages), actual, predicted,
               ("%.2f" % ratio) if ratio else "n/a", legacy,
               ("%.1f" % (actual / legacy)) if legacy else "n/a"))
    return samples


# ---------------------------------------------------------------- 4. 反推系数
def solve(samples, current):
    """反推建议系数。

    模型：真实 ≈ safety × (base + M×perMsg + cjk×n_cjk + ascii×n_other)

    四个参数需要四组独立信息才能解出来：
      ① ascii —— 英文短/长两点相减，消掉所有固定项，只剩斜率
      ② cjk   —— 中文短/长两点相减，同理
      ③ base + perMsg        —— 单消息探针的截距（多个单消息探针取平均更稳）
      ④ base + M×perMsg      —— 消息数最多的探针的截距
    ③④ 联立就把 base（一次性）与 perMsg（每条）分开了 —— 这两项分不开的话，
    多轮对话会被严重高估或低估。

    所有真实值都先除以 safety-factor：校准的是系数本身，
    安全系数是另一个独立概念（有意保留的保守量），不该被校准吞掉。
    """
    def by_name(n):
        for s in samples:
            if s["name"] == n:
                return s
        return None

    def content_tokens(s, cjk_coeff, ascii_coeff):
        """探针内容（含 role）折算出的 token 数。"""
        return cjk_coeff * s["cjk"] + ascii_coeff * s["other"]

    suggested = dict(current)
    notes = []
    safety = current["safetyFactor"]
    cjk_coeff = current["cjkPerChar"]
    ascii_coeff = current["asciiPerChar"]

    cn_short, cn_long = by_name("中文-短"), by_name("中文-长")
    en_short, en_long = by_name("英文-短"), by_name("英文-长")

    # ① ascii 斜率（英文短/长两点）
    if en_short and en_long and en_long["other"] != en_short["other"]:
        dn = en_long["other"] - en_short["other"]
        ascii_coeff = ((en_long["actual"] - en_short["actual"]) / safety) / dn
        if ascii_coeff > 0:
            suggested["asciiPerChar"] = ascii_coeff
            notes.append("ascii-per-char：由英文短/长两点反推 = %.4f（原 %.4f）"
                         % (ascii_coeff, current["asciiPerChar"]))

    # ② cjk 斜率（中文短/长两点）
    if cn_short and cn_long and cn_long["cjk"] != cn_short["cjk"]:
        dn = cn_long["cjk"] - cn_short["cjk"]
        cjk_coeff = ((cn_long["actual"] - cn_short["actual"]) / safety) / dn
        if cjk_coeff > 0:
            suggested["cjkPerChar"] = cjk_coeff
            notes.append("cjk-per-char：由中文短/长两点反推 = %.4f（原 %.4f）"
                         % (cjk_coeff, current["cjkPerChar"]))

    # ③ 单消息固定项 K1 = base + perMsg（多个单消息探针取平均）
    singles = [s for s in samples if len(s["messages"]) == 1]
    k1 = None
    if singles:
        vals = [s["actual"] / safety - content_tokens(s, cjk_coeff, ascii_coeff)
                for s in singles]
        k1 = sum(vals) / len(vals)
        notes.append("单消息固定项 base + perMsg ≈ %.4f（%d 个单消息探针平均）"
                     % (k1, len(vals)))

    # ④ 消息最多的探针：KM = base + M×perMsg，与 K1 联立
    multi = None
    for s in samples:
        if len(s["messages"]) > 1 and (multi is None
                                       or len(s["messages"]) > len(multi["messages"])):
            multi = s
    if k1 is not None and multi is not None:
        m = len(multi["messages"])
        km = multi["actual"] / safety - content_tokens(multi, cjk_coeff, ascii_coeff)
        per_msg = (km - k1) / (m - 1)
        base = k1 - per_msg
        if base > 0:
            suggested["baseOverhead"] = base
            notes.append("base-overhead：与 perMsg 联立解出 = %.4f（原 %.4f）"
                         % (base, current.get("baseOverhead") or 0.0))
        if per_msg >= 0:
            suggested["perMessageOverhead"] = per_msg
            notes.append("per-message-overhead：同上联立解出 = %.4f（原 %.4f）"
                         % (per_msg, current["perMessageOverhead"]))
    return suggested, notes


# ---------------------------------------------------------------- 5. 断言
def verify(samples, coeff):
    log()
    log("=" * 72)
    log("Step 4 · 断言（估算 / 真实 应落在 %.2f~%.2f）" % (1 - TOLERANCE, 1 + TOLERANCE))
    log("=" * 72)
    checks = []
    if not samples:
        log("  [!] 没有采集到任何样本，无法断言")
        return False

    for s in samples:
        r = s["ratio"]
        good = r is not None and (1 - TOLERANCE) <= r <= (1 + TOLERANCE)
        checks.append(("%-6s 估算/真实 = %s（真实 %s，估算 %s）"
                       % (s["name"], ("%.2f" % r) if r else "n/a", s["actual"], s["predicted"]),
                       good))

    # 专项断言：中文不能被旧公式那套逻辑处理
    cn = [s for s in samples if s["cjk"] > 0 and s["other"] <= 8]
    for s in cn:
        legacy_ratio = s["actual"] / s["legacy"] if s["legacy"] else None
        checks.append(("%-6s 相对旧公式的改进倍数 = %s"
                       % (s["name"], ("%.1fx" % legacy_ratio) if legacy_ratio else "n/a"),
                       s["legacy"] > 0 and s["actual"] / max(1, s["legacy"]) > 1.5))

    ok = True
    for name, good in checks:
        log("  [%s] %s" % ("PASS" if good else "FAIL", name))
        ok = ok and good
    return ok


def main():
    proc = None
    tail = None
    ok = False
    try:
        log("=" * 72)
        log("llm-gateway token 估算校准  批次=%s" % RUN_TAG)
        log("=" * 72)

        if tcp_ok():
            log("  [!] 端口 %d 已被占用，先停掉占用进程再跑" % PORT)
            return
        log("  端口 %d 空闲，可以启动" % PORT)

        proc, tail = start_app()
        if not wait_ready(tail):
            log("  [!] 服务未就绪，中止")
            return

        code, pricing = api_get("/admin/pricing")
        if code != 200 or not pricing or "tokenEstimate" not in pricing:
            log("  [!] 读不到 /admin/pricing 的 tokenEstimate（HTTP %s）：%s"
                % (code, json.dumps(pricing, ensure_ascii=False)[:200] if pricing else None))
            log("      该字段由 AdminController 暴露，缺少它说明跑的不是本批代码。")
            return
        coeff = pricing["tokenEstimate"]
        log("  /admin/pricing 回读生效系数: %s" % json.dumps(coeff, ensure_ascii=False))

        samples = collect(coeff)
        ok = verify(samples, coeff)

        suggested, notes = solve(samples, coeff)
        log()
        log("=" * 72)
        log("Step 5 · 建议系数（实测反推，可回填 application.yml）")
        log("=" * 72)
        if notes:
            for n in notes:
                log("  · %s" % n)
        else:
            log("  （样本不足，未能反推；需要纯中文短/长与纯英文短/长四组都成功）")
        log()
        log("  gateway:")
        log("    token-estimate:")
        log("      base-overhead: %.4f" % suggested.get("baseOverhead", 0.0))
        log("      cjk-per-char: %.4f" % suggested["cjkPerChar"])
        log("      ascii-per-char: %.4f" % suggested["asciiPerChar"])
        log("      per-message-overhead: %.4f" % suggested["perMessageOverhead"])
        log("      safety-factor: %.2f" % suggested["safetyFactor"])
    finally:
        if proc and proc.poll() is None:
            log()
            log("Step 6 · 关闭服务")
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True)
            time.sleep(3)
            log("  已发送 taskkill /F /T，端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))
        log()
        log("=== 结果: %s ===" % ("估算精度达标 ✅" if ok else "未达标 ❌（见上方 FAIL 项）"))
        flush()


main()
