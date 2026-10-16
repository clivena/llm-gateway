# -*- coding: utf-8 -*-
"""
llm-gateway 两级缓存验证 —— 证明缓存**真的会命中**

它回答一个问题：**L1 精确缓存是不是在工作？**

为什么值得单独一个脚本：在 P4 之前，这个项目宣称有语义缓存，但真机上一次都没命中过 ——
语义缓存依赖 embedding，而当时的供应商（DeepSeek）没有 /embeddings 端点，
于是 lookup 恒空、write 恒不写。**"缓存开着"与"缓存命中"是两件事**，
而只有后者能省钱。没有这个脚本，那次静默失效可以一直存在下去。

做法（每组用独立租户，互不干扰）：
  1) 每组发两次请求：先用 A 建立缓存，再用 B 查询（B 与 A 的关系由用例定义）
  2) 读响应头 X-Cache-Hit 判断是否命中 —— 命中信息刻意不进 body，
     因为 body 必须是标准 OpenAI 形状（任何客户端可直连）
  3) 三重交叉验证命中为真：
       · X-Cache-Hit: true
       · 第二次明显更快（没有上游调用）
       · 第二次 usage 为 0/0（没花钱）
     三者同时成立才算命中 —— 只看响应头的话，一个"永远返回 true 的 bug"也能骗过测试
  4) 账本侧核对：/admin/ledger/recent 里存在 cache_hit=true 的行

用例设计上刻意包含**不该命中**的两组（标点不同、内容不同）：
精确缓存的失败模式是"错答案"而不是"漏命中"，所以"不该命中的确实没命中"
和"该命中的命中了"同样重要。标点那组尤其关键 —— 它记录了我们的取舍：
"这是对的"与"这是对的？"是两个问题，模糊匹配留给 L2 语义层（它有阈值兜着）。

用法::

    docker compose up -d redis postgres
    python scripts/check-cache-layers.py

可覆盖的环境变量：GATEWAY_DIR / CACHE_LOG / GATEWAY_PORT / CACHE_MODEL /
CACHE_MVN_CMD / CACHE_MAVEN_REPO / JAVA_HOME。
"""
import collections
import http.client
import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(os.environ.get("GATEWAY_DIR") or Path(__file__).resolve().parents[1])
LOG = Path(os.environ.get("CACHE_LOG") or (ROOT / "out" / "check-cache-layers.log"))
PORT = int(os.environ.get("GATEWAY_PORT") or 8080)
MODEL = os.environ.get("CACHE_MODEL") or "deepseek-flash"
# 模型隔离用例要用的第二个模型（必须也被 provider 支持）
ALT_MODEL = os.environ.get("CACHE_ALT_MODEL") or "deepseek-v4-pro"

RUN_TAG = time.strftime("%H%M%S")
FEATURE = "cache-check"

# (名称, 建立缓存的提问, 用来查询的提问, 是否期望命中, 期望/原因说明)
CASES = [
    ("完全相同", "什么是语义缓存", "什么是语义缓存", True,
     "字面完全相同，最基础的命中"),
    ("首尾空格", "什么是语义缓存", "   什么是语义缓存   ", True,
     "trim 是安全规范化：首尾空白不携带语义"),
    ("连续空白", "什么是语义缓存 和精确缓存", "什么是语义缓存   和精确缓存", True,
     "空白折叠是安全规范化：换行/多空格不携带语义"),
    ("英文大小写", "What Is A Cache?", "what is a cache?", True,
     "ASCII 大小写归一：Java 与 java 指同一个东西"),
    ("标点不同", "什么是语义缓存", "什么是语义缓存？", False,
     "刻意不剥标点：这是两个问题，模糊匹配留给 L2（它有阈值兜着）"),
    ("内容不同", "什么是语义缓存", "什么是精确缓存", False,
     "不同问题绝不该共享答案"),
]

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
    if os.environ.get("CACHE_MVN_CMD"):
        return os.environ["CACHE_MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo() -> str:
    if os.environ.get("CACHE_MAVEN_REPO"):
        return os.environ["CACHE_MAVEN_REPO"]
    local = Path.home() / ".workbuddy/binaries/maven/repo"
    return str(local) if local.exists() else str(Path.home() / ".m2" / "repository")


MVN = _pick_mvn()
REPO = _pick_repo()
JAVA_HOME = os.environ.get("JAVA_HOME", "")


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


def ask(tenant, question, model=None, timeout=300):
    """发一次非流式请求，返回 (状态码, 是否命中, 耗时秒, usage, 响应体 dict)。"""
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    payload = {
        "model": model or MODEL,
        "stream": False,
        "temperature": 0.0,
        # ★★ max_tokens 必须给足 —— 这是本项目踩过的最大一次"看起来像缓存 bug"的坑。
        #
        # 实测（见 scripts/probe-upstream-shape.py 的原始数据）：DeepSeek 这两个模型
        # 都是**推理型**，响应里带 reasoning_content 字段，而 max_tokens 是
        # "推理 + 正文"的**总**预算 —— 不是只算正文。
        #
        #   模型              max_tokens=512   1024    2048     4096
        #   deepseek-flash     空(仅推理)    空(仅推理) 217字   1220字
        #   deepseek-v4-pro    空(仅推理)    1514字    2483字   2639字
        #
        # 推理 token 数还高度不确定（flash 在 512/1024/1935/1740 之间跳），
        # 所以"看起来够大"的 1024 也可能一个正文字都不出。
        # 一旦 content 为空 → 缓存没东西可写 → 现象是"配置全开、一次都不命中"，
        # 与"缓存彻底失效"的表现**一模一样**，极易误判（本次就误判了一轮）。
        #
        # 取 4096：两个模型都实测能自然收尾（finish_reason=stop）。
        # 本脚本只发十几个短请求，预算放大带来的成本可忽略。
        "max_tokens": 4096,
        "feature": FEATURE,
        "messages": [{"role": "user", "content": question}],
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    t0 = time.time()
    try:
        conn.request("POST", "/v1/chat/completions", body=body, headers={
            "Content-Type": "application/json",
            "X-Api-Key": tenant,
        })
        resp = conn.getresponse()
        raw = resp.read()
        status = resp.status
        hit_header = resp.getheader("X-Cache-Hit")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    elapsed = time.time() - t0
    try:
        data = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        data = {}
    return (status, (str(hit_header).lower() == "true"), elapsed,
            (data or {}).get("usage"), data or {})


def answer_of(body):
    """从 OpenAI 形状的响应里取正文。取不到返回空串。"""
    try:
        return ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    except Exception:
        return ""


# ---------------------------------------------------------------- 服务生命周期
def start_app():
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"

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
    log("Step 1 · 启动服务（端口 %d）" % PORT)
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


# ---------------------------------------------------------------- 2. 逐组验证
def run_cases():
    log()
    log("=" * 72)
    log("Step 2 · 逐组验证（每组独立租户，避免组间互相污染）")
    log("=" * 72)
    results = []

    for idx, (name, first_q, second_q, expect_hit, why) in enumerate(CASES):
        tenant = "cache-check-%s-%d" % (RUN_TAG, idx)
        log()
        log("  ── 用例 %d: %s ──" % (idx + 1, name))
        log("     期望: %s（%s）" % ("命中" if expect_hit else "不命中", why))

        try:
            s1, h1, e1, u1, b1 = ask(tenant, first_q)
            content1 = answer_of(b1)
            log("     第1次 [%s] 缓存命中=%s 耗时=%.0fms usage=%s 正文=%d字"
                % (first_q[:24], h1, e1 * 1000, u1, len(content1)))
            if s1 != 200:
                log("     [!] 第一次请求 HTTP %s，跳过本组" % s1)
                continue
            # 前置断言：没有正文就无从写入缓存，后面的"是否命中"全部无意义。
            # 这一条是实测踩出来的 —— 上游是**推理型**模型，max_tokens 是
            # "推理+正文"的总预算，给少了就只出 reasoning_content、content 为空。
            # 而现象（配置全开、一次都不命中）与"缓存失效"一模一样，极易误判。
            # 根治办法见 ask() 里那张实测表；这里做成硬断言，避免下次又误判。
            if not content1.strip():
                log("     [!] 第1次返回了空推理型响应（content=\"\"）→ 缓存没东西可写")
                log("         根因：max_tokens 是「推理 + 正文」总预算，被 reasoning 吃光了。")
                log("         自查：python scripts/probe-upstream-shape.py 看当前预算够不够。")
                results.append((name, expect_hit, False, "探针未产生正文（推理吃光预算）"))
                continue

            # 缓存写入是 fire-and-forget 的（不能拖住响应），给它一点时间落 Redis
            time.sleep(0.5)

            s2, h2, e2, u2, b2 = ask(tenant, second_q)
            log("     第2次 [%s] 缓存命中=%s 耗时=%.0fms usage=%s"
                % (second_q[:24], h2, e2 * 1000, u2))
            if s2 != 200:
                log("     [!] 第二次请求 HTTP %s，跳过本组" % s2)
                continue

            # 三重交叉验证：只看响应头的话，"永远返回 true 的 bug"也能骗过测试
            fast = e2 < max(e1 * 0.5, 0.5)
            free = bool(u2) and u2.get("prompt_tokens") == 0 and u2.get("completion_tokens") == 0
            if expect_hit:
                good = h2 and fast and free
                detail = "命中=%s 更快=%s(%.0f→%.0fms) 未计费=%s" % (h2, fast, e1 * 1000, e2 * 1000, free)
            else:
                good = (not h2) and (not free)
                detail = "未命中=%s 走到了上游=%s" % (not h2, not free)
            log("     判定: [%s] %s" % ("PASS" if good else "FAIL", detail))
            results.append((name, expect_hit, good, detail))
        except Exception as e:
            log("     [!] 异常: %r" % e)
            results.append((name, expect_hit, False, "异常 %r" % e))
    return results


# ---------------------------------------------------------------- 3. 模型隔离
def run_model_isolation():
    log()
    log("=" * 72)
    log("Step 3 · 缓存按模型隔离（namespace = 租户:模型）")
    log("=" * 72)
    tenant = "cache-model-%s" % RUN_TAG
    q = "什么是模型隔离测试"

    try:
        s1, _h1, _e1, _u1, b1 = ask(tenant, q, model=MODEL)
        if not answer_of(b1).strip():
            log("  [!] 第1次（%s）返回空正文，本组无法验证 —— 同前，推理吃光了 max_tokens" % MODEL)
            log("      自查：python scripts/probe-upstream-shape.py")
            return [("模型隔离", False, False, "探针未产生正文（推理吃光预算）")]
        time.sleep(0.5)
        s2, h2, e2, u2, _b2 = ask(tenant, q, model=ALT_MODEL)
        log("  用 %s 建立缓存后，改用 %s 查询同一问题：" % (MODEL, ALT_MODEL))
        log("    命中=%s 耗时=%.0fms usage=%s" % (h2, e2 * 1000, u2))
        # 隔离失效时的后果是"强模型的答案被弱模型请求复用"，路由策略被缓存架空
        good = (not h2) and bool(u2) and u2.get("prompt_tokens", 0) > 0
        log("  [%s] 换模型后未命中（否则强模型答案会被弱模型请求白拿）"
            % ("PASS" if good else "FAIL"))
        return [("模型隔离", False, good, "换模型未命中=%s" % (not h2))]
    except Exception as e:
        log("  [!] 异常: %r" % e)
        return [("模型隔离", False, False, "异常 %r" % e)]


# ---------------------------------------------------------------- 4. 账本核对
def check_ledger():
    log()
    log("=" * 72)
    log("Step 4 · 账本核对（命中也要落账，只是金额为 0）")
    log("=" * 72)
    time.sleep(3)  # 落账是异步的（Kafka/Postgres 链路），给它一点时间
    code, rows = api_get("/admin/ledger/recent?limit=200")
    if code != 200 or not isinstance(rows, list):
        log("  （跳过：/admin/ledger/recent 返回 HTTP %s）" % code)
        return []
    mine = [r for r in rows if r.get("feature") == FEATURE]
    hit_rows = [r for r in mine if r.get("cacheHit") is True or r.get("cache_hit") is True]
    log("  本次请求落账 %d 行，其中 cache_hit=true 的 %d 行" % (len(mine), len(hit_rows)))
    for r in hit_rows[:4]:
        log("    · trace=%s model=%s prompt=%s completion=%s cost=%s"
            % (str(r.get("traceId") or r.get("trace_id"))[:12], r.get("model"),
               r.get("promptTokens"), r.get("completionTokens"), r.get("cost")))
    good = len(hit_rows) > 0
    log("  [%s] 命中请求在账本里留下了 cache_hit=true 的行"
        % ("PASS" if good else "FAIL"))
    return [("账本记录命中", True, good, "cache_hit 行数 = %d" % len(hit_rows))]


def main():
    proc = None
    tail = None
    ok = False
    try:
        log("=" * 72)
        log("llm-gateway 两级缓存验证  批次=%s" % RUN_TAG)
        log("=" * 72)
        log("  模型: %s（隔离用例对照 %s）" % (MODEL, ALT_MODEL))

        if tcp_ok():
            log("  [!] 端口 %d 已被占用，先停掉占用进程再跑" % PORT)
            return
        log("  端口 %d 空闲，可以启动" % PORT)

        proc, tail = start_app()
        if not wait_ready(tail):
            log("  [!] 服务未就绪，中止")
            return

        code, pricing = api_get("/admin/pricing")
        if code == 200 and pricing:
            log("  /admin/pricing 缓存配置: enabled=%s exact=%s semantic=%s"
                % (pricing.get("cacheEnabled"), pricing.get("cacheExactEnabled"),
                   pricing.get("cacheSemanticEnabled")))
            if not pricing.get("cacheExactEnabled"):
                log("  [!] L1 精确缓存已关闭，本脚本的命中断言必然失败。")
                log("      请在配置里打开 gateway.cache.exact-enabled")

        results = run_cases()
        results += run_model_isolation()
        results += check_ledger()

        log()
        log("=" * 72)
        log("Step 5 · 汇总")
        log("=" * 72)
        ok = bool(results)
        for name, expect_hit, good, detail in results:
            log("  [%s] %-10s 期望=%-4s %s"
                % ("PASS" if good else "FAIL", name,
                   "命中" if expect_hit else "不命中", detail))
            ok = ok and good
    finally:
        if proc and proc.poll() is None:
            log()
            log("Step 6 · 关闭服务")
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True)
            time.sleep(3)
            log("  已发送 taskkill /F /T，端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))
        log()
        log("=== 结果: %s ===" % ("缓存验证全部通过 ✅" if ok else "未通过 ❌（见上方 FAIL 项）"))
        flush()


main()
