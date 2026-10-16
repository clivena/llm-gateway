# -*- coding: utf-8 -*-
"""
llm-gateway F0 验证 —— 推理型模型的失败语义 + 配额 reserve-then-settle

它回答两个问题：

  ① **上游"给了响应但没给答案"时，客户端能不能知道？**
     用例是推理型模型最典型的失败形态：预算被 reasoning 吃光，
     返回 finish_reason=length + content=""。F0 之前网关会把它报成
     finish_reason="stop"（硬编码），客户端连猜的机会都没有。

  ② **配额记的是不是真实消耗？**
     F0 之前配额只有预扣没有结算，记的永远是"prompt 估算之和"，
     而实测 completion 是 prompt 的 20~100 倍。
     本脚本直接拿"配额剩余量的变化"去对账本的真实 tokens。

为什么值得一个独立脚本：这两条都**不会报错**。上游 HTTP 200、
网关主链路正常、账本正常落账，只有翻配额面板或读响应体才发现数字不对。
单测能守住分支，但守不住"我们对上游实际行为的假设"——
比如"max_tokens=64 时上游到底返回什么"就只有真机能回答。

用法::

    docker compose up -d redis postgres
    python scripts/check-f0-failure-semantics.py

可覆盖的环境变量：GATEWAY_DIR / F0_LOG / GATEWAY_PORT / F0_MODEL /
F0_MVN_CMD / F0_MAVEN_REPO / JAVA_HOME。
"""
import collections
import http.client
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(os.environ.get("GATEWAY_DIR") or Path(__file__).resolve().parents[1])
LOG = Path(os.environ.get("F0_LOG") or (ROOT / "out" / "check-f0-failure-semantics.log"))
PORT = int(os.environ.get("GATEWAY_PORT") or 8080)
MODEL = os.environ.get("F0_MODEL") or "deepseek-flash"

RUN_TAG = time.strftime("%H%M%S")
FEATURE = "f0-check"

# 实测（scripts/probe-upstream-shape.py）：这两个模型都是推理型，
# max_tokens 是"推理 + 正文"的总预算。64 必然被 reasoning 吃光 → length + 空正文。
# 这正是我们要复现的失败形态，所以这里刻意用一个小预算。
TINY_BUDGET = 64
# 让请求能自然收尾所需的预算（flash 实测 4096 能 finish_reason=stop）。
FULL_BUDGET = 4096

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


def _pick_mvn():
    if os.environ.get("F0_MVN_CMD"):
        return os.environ["F0_MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo():
    if os.environ.get("F0_MAVEN_REPO"):
        return os.environ["F0_MAVEN_REPO"]
    local = Path.home() / ".workbuddy/binaries/maven/repo"
    return str(local) if local.exists() else str(Path.home() / ".m2" / "repository")


MVN = _pick_mvn()
REPO = _pick_repo()
JAVA_HOME = os.environ.get("JAVA_HOME", "")

NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


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


def api_get(path, timeout=15):
    try:
        r = NO_PROXY_OPENER.open("http://127.0.0.1:%d%s" % (PORT, path), timeout=timeout)
        return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as he:
        return he.code, he.read().decode("utf-8", "replace")
    except Exception as e:
        return -1, repr(e)


def api_json(path, timeout=15):
    code, raw = api_get(path, timeout)
    try:
        return code, json.loads(raw)
    except Exception:
        return code, None


def ask(tenant, question, max_tokens, timeout=300):
    """非流式请求，返回 (状态码, 是否命中, 耗时秒, 响应头, 响应体 dict)。"""
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    payload = {
        "model": MODEL,
        "stream": False,
        "temperature": 0.0,
        "max_tokens": max_tokens,
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
        headers = {k.lower(): v for k, v in resp.getheaders()}
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
    return status, str(headers.get("x-cache-hit", "")).lower() == "true", elapsed, headers, data or {}


def ask_stream(tenant, question, max_tokens, timeout=300):
    """流式请求，返回 (状态码, 所有分片 dict 列表, 是否见到 [DONE], 原始响应体)。"""
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    payload = {
        "model": MODEL,
        "stream": True,
        "temperature": 0.0,
        "max_tokens": max_tokens,
        "stream_options": {"include_usage": True},
        "feature": FEATURE,
        "messages": [{"role": "user", "content": question}],
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    chunks = []
    done = False
    status = None
    raw_all = bytearray()
    try:
        conn.request("POST", "/v1/chat/completions", body=body, headers={
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "X-Api-Key": tenant,
        })
        resp = conn.getresponse()
        status = resp.status
        buf = b""
        while True:
            block = resp.read(1)
            if not block:
                break
            raw_all += block
            buf += block
            if buf.endswith(b"\n\n"):
                for line in buf.decode("utf-8", "replace").splitlines():
                    if not line.startswith("data: "):
                        continue
                    payload_line = line[6:].strip()
                    if payload_line == "[DONE]":
                        done = True
                        continue
                    try:
                        chunks.append(json.loads(payload_line))
                    except Exception:
                        pass
                buf = b""
    except Exception as e:
        log("     [!] 流式读取异常: %r" % e)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return status, chunks, done, raw_all.decode("utf-8", "replace")


def quota_used(tenant):
    """读该租户当前周期已用配额。读不到返回 None。"""
    code, data = api_json("/admin/quota/%s" % tenant)
    if code == 200 and isinstance(data, dict):
        return data.get("usedTokens")
    return None


def prometheus_counters():
    """从 /actuator/prometheus 取 F0 两个计数器的当前值（缺失视为 0）。"""
    code, raw = api_get("/actuator/prometheus")
    if code != 200:
        return None
    out = {}
    for name in ("llmgw_upstream_empty_total", "llmgw_upstream_truncated_total",
                 "llmgw_quota_settled_total", "llmgw_quota_settle_failed_total"):
        m = re.search(r"^%s\s+([0-9.eE+-]+)$" % re.escape(name), raw, re.M)
        out[name] = float(m.group(1)) if m else 0.0
    return out


def answer_of(body):
    try:
        return ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    except Exception:
        return ""


def finish_reason_of(body):
    try:
        return (body.get("choices") or [{}])[0].get("finish_reason")
    except Exception:
        return None


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

    proc = subprocess.Popen(cmd, cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            bufsize=1, universal_newlines=False)
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


# ---------------------------------------------------------------- 检查 1：非流式透传
def check_non_stream_finish_reason():
    log()
    log("=" * 72)
    log("检查 1 · 非流式 finish_reason 透传（预算被 reasoning 吃光的形态）")
    log("=" * 72)
    tenant = "f0-finish-%s" % RUN_TAG
    q = "什么是语义缓存"

    before = prometheus_counters()
    status, _hit, elapsed, _h, body = ask(tenant, q, TINY_BUDGET)
    after = prometheus_counters()

    content = answer_of(body)
    reason = finish_reason_of(body)
    usage = body.get("usage") or {}
    log("  HTTP=%s 耗时=%.0fms max_tokens=%d" % (status, elapsed * 1000, TINY_BUDGET))
    log("  finish_reason = %r   正文 = %d 字" % (reason, len(content)))
    log("  usage = %s" % usage)

    ok = status == 200
    if not ok:
        log("  [FAIL] 请求未成功")
        return [("非流式 finish_reason 透传", False, "HTTP %s" % status)], before, after

    # 核心断言：透传，不是硬编码 "stop"
    if reason == "length":
        log("  [PASS] finish_reason 原样透传为 'length'（F0 之前这里恒为 'stop'）")
        pass_tx = True
    else:
        log("  [FAIL] finish_reason = %r，期望 'length'" % reason)
        log("         若上游这次正常出正文，说明预算够用了，把 TINY_BUDGET 调小")
        pass_tx = False

    # 空正文判定（上游的具体行为，作为观察项：flash 在小预算下必然空）
    empty = not content.strip()
    log("  %s 正文为空（推理型模型的预期形态）" % ("[OK]" if empty else "[!] "))
    if not empty:
        log("         注意：本次上游出了正文，检查 1 的形态没复现出来")

    ok = pass_tx
    return [("非流式 finish_reason 透传", ok,
             "finish_reason=%r 正文=%d字" % (reason, len(content)))], before, after


# ---------------------------------------------------------------- 检查 2：流式收尾帧
def check_stream_finish_frame():
    log()
    log("=" * 72)
    log("检查 2 · 流式收尾帧（客户端的 [DONE] 之外还要能拿到原因）")
    log("=" * 72)
    tenant = "f0-stream-%s" % RUN_TAG
    q = "什么是精确缓存"

    status, chunks, done, raw = ask_stream(tenant, q, TINY_BUDGET)

    text_frames = [c for c in chunks if ((c.get("choices") or [{}])[0].get("delta") or {}).get("content")]
    finish_frames = [c for c in chunks
                     if (c.get("choices") or [{}])[0].get("finish_reason")]
    reasons = [(c["choices"][0]["finish_reason"]) for c in finish_frames]

    log("  HTTP=%s 文本分片=%d 收尾帧=%d [DONE]=%s"
        % (status, len(text_frames), len(finish_frames), done))
    log("  收尾帧的 finish_reason = %s" % reasons)

    ok = status == 200 and done and len(finish_frames) >= 1 and "length" in reasons
    if not ok:
        if not done:
            log("  [FAIL] 没收到 [DONE]")
        elif not finish_frames:
            log("  [FAIL] 没收到收尾帧 —— 上游的 finish_reason 被当成「没有 content 的分片」丢掉了")
        else:
            log("  [FAIL] finish_reason = %s，期望含 'length'" % reasons)
        # 非 200 必须把原始响应体打出来：502 既不说明是上游拒绝还是网关内部异常，
        # 不打印 body 就只能靠再跑一遍 + 翻服务日志去猜。
        if status != 200:
            log("  ── 原始响应体（前 600 字）──")
            log("  " + raw[:600].replace("\n", "\n  "))
    else:
        log("  [PASS] 收尾帧带 finish_reason='length'，客户端能分辨「被截断」")

    # 收尾帧的 delta 必须是空对象，不能带 content
    if finish_frames:
        d = finish_frames[0]["choices"][0].get("delta")
        if isinstance(d, dict) and len(d) == 0:
            log("  [PASS] 收尾帧 delta 是空对象（不会被客户端当成一段空回复）")
        else:
            log("  [FAIL] 收尾帧 delta = %r，期望空对象" % (d,))
            ok = False

    return [("流式收尾帧", ok, "finish_reason=%s" % reasons)]


# ---------------------------------------------------------------- 检查 3 & 4：结算
def check_quota_settle():
    log()
    log("=" * 72)
    log("检查 3 · 配额结算覆盖 completion（reserve-then-settle）")
    log("=" * 72)
    tenant = "f0-settle-%s" % RUN_TAG
    q = "什么是成本账本"

    used0 = quota_used(tenant)
    if used0 is None:
        log("  [SKIP] /admin/quota/%s 读不到（服务或 Redis 不可用）" % tenant)
        return [("配额结算覆盖 completion", False, "配额端点不可读")]

    log("  请求前已用配额 = %s" % used0)
    status, hit, elapsed, _h, body = ask(tenant, q, FULL_BUDGET)
    usage = body.get("usage") or {}
    prompt = int(usage.get("prompt_tokens") or 0)
    completion = int(usage.get("completion_tokens") or 0)

    log("  HTTP=%s 耗时=%.0fms 缓存命中=%s" % (status, elapsed * 1000, hit))
    log("  上游真实 usage: prompt=%d completion=%d total=%d" % (prompt, completion, prompt + completion))
    if hit:
        log("  [!] 本次竟然命中了缓存（租户是新的，不该命中）—— 结果不可信")
        return [("配额结算覆盖 completion", False, "新租户意外命中缓存")]

    time.sleep(0.5)
    used1 = quota_used(tenant)
    delta = (used1 or 0) - (used0 or 0)
    log("  请求后已用配额 = %s（增量 = %d）" % (used1, delta))

    expected = prompt + completion
    # 容差：settle 的 delta 计算是整数，正常应该**完全相等**。
    # 留 2 的余量只为吸收"服务端另有一次同租户请求"这类偶发。
    if abs(delta - expected) <= 2 and expected > 0:
        log("  [PASS] 配额增量 %d == 真实 usage %d（含 completion %d）" % (delta, expected, completion))
        log("         F0 之前这里只会是 prompt 侧的估算值（约 %d）" % prompt)
        ok = True
    else:
        log("  [FAIL] 配额增量 %d != 真实 usage %d" % (delta, expected))
        if delta <= prompt + 2:
            log("         ≈(或小于) prompt 侧，说明结算没生效，配额仍只覆盖 prompt")
        ok = False

    # ---- 检查 4：命中全额退还 ----
    log()
    log("=" * 72)
    log("检查 4 · 缓存命中全额退还预扣")
    log("=" * 72)
    used_before_hit = quota_used(tenant)
    status2, hit2, elapsed2, _h2, body2 = ask(tenant, q, FULL_BUDGET)
    time.sleep(0.5)
    used_after_hit = quota_used(tenant)
    delta2 = (used_after_hit or 0) - (used_before_hit or 0)

    log("  HTTP=%s 耗时=%.0fms 缓存命中=%s" % (status2, elapsed2 * 1000, hit2))
    log("  配额 %s → %s（增量 = %d）" % (used_before_hit, used_after_hit, delta2))
    log("  第二次响应 usage = %s" % (body2.get("usage") or {}))

    if not hit2:
        log("  [FAIL] 第二次没命中缓存，检查 4 无法验证（检查 3 的答案可能为空，缓存拒写）")
        ok4 = False
    elif delta2 == 0:
        log("  [PASS] 命中不消耗配额 —— 预扣被全额退还（F0 之前会白扣一笔 prompt 估算）")
        ok4 = True
    else:
        log("  [FAIL] 命中却仍扣了 %d 配额，预扣没有退还" % delta2)
        ok4 = False

    return [("配额结算覆盖 completion", ok, "增量=%d 真实=%d" % (delta, expected)),
            ("缓存命中全额退还预扣", ok4, "命中增量=%d" % delta2)]


# ---------------------------------------------------------------- 检查 5：指标
def check_metrics(before, after):
    log()
    log("=" * 72)
    log("检查 5 · F0 指标（上游失败必须可告警，不能只靠日志）")
    log("=" * 72)
    if before is None or after is None:
        log("  [SKIP] /actuator/prometheus 不可用")
        return [("F0 指标计数", False, "prometheus 端点不可读")]

    results = []
    for name in ("llmgw_upstream_empty_total", "llmgw_upstream_truncated_total"):
        b, a = before.get(name, 0.0), after.get(name, 0.0)
        grew = a > b
        log("  %-34s %s → %s  %s" % (name, b, a, "[PASS]" if grew else "[FAIL]"))
        results.append((name, grew, "%s→%s" % (b, a)))

    # 结算计数：检查 3/4 之后必然涨过。这里只做"存在性"确认。
    now = prometheus_counters() or {}
    settled = now.get("llmgw_quota_settled_total", 0.0)
    failed = now.get("llmgw_quota_settle_failed_total", 0.0)
    log("  %-34s %s  %s" % ("llmgw_quota_settled_total", settled,
                            "[PASS]" if settled > 0 else "[FAIL]"))
    # 结算失败数必须单列：它 >0 就意味着配额在**系统性失真**，
    # 而现象是"配额面板数字偏小"—— 不看这个计数器根本发现不了。
    log("  %-34s %s  %s" % ("llmgw_quota_settle_failed_total", failed,
                            "[PASS]" if failed == 0 else "[FAIL]"))
    results.append(("llmgw.quota.settled", settled > 0, "=%s" % settled))
    results.append(("llmgw.quota.settle.failed==0", failed == 0, "=%s" % failed))
    return results


def deps_missing():
    """
    检查 Redis / Postgres 容器是否在跑。返回缺失的容器名列表；无法判定返回 None。

    为什么值得一个前置检查：本脚本的配额断言依赖 Redis。Redis 挂掉时
    QuotaService 会 **fail-open**（放行 + 记日志），于是配额恒为 0、结算恒失败 ——
    三条断言全红，而它们看起来像"结算没实现"。本次实测就踩了这个坑：
    docker 容器在跑脚本前 15 分钟停了，我先去读代码，白花了一轮。

    环境问题必须在**开始前**和代码问题分开，否则每次都要重新推一遍。
    """
    try:
        out = subprocess.run(["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
                             capture_output=True, text=True, timeout=30).stdout
    except Exception as e:
        log("  [!] 无法执行 docker ps: %r" % e)
        return None
    missing = []
    for c in ("llm-gateway-redis-1", "llm-gateway-postgres-1"):
        if not any(line.startswith(c) and "Up" in line for line in out.splitlines()):
            missing.append(c)
    return missing


def dump_tail(tail, n=80):
    """把服务日志末尾打出来。失败时唯一能区分"上游拒绝"与"网关内部异常"的东西。"""
    if not tail:
        return
    log()
    log("─" * 72)
    log("服务日志末尾 %d 行" % n)
    log("─" * 72)
    for line in list(tail)[-n:]:
        log("  " + line)


def main():
    proc = None
    tail = None
    ok = False
    try:
        log("=" * 72)
        log("llm-gateway F0 验证（推理型模型失败语义 + 配额结算）  批次=%s" % RUN_TAG)
        log("=" * 72)
        log("  模型: %s   小预算=%d / 完整预算=%d" % (MODEL, TINY_BUDGET, FULL_BUDGET))

        missing = deps_missing()
        if missing:
            log()
            log("  [中止] 依赖容器未运行: %s" % ", ".join(missing))
            log("         配额部分（检查 3/4/5）依赖 Redis，缺它时 QuotaService 是 fail-open，")
            log("         表现为「配额恒为 0」—— 会被误读成「结算没实现」，白查一轮。")
            log("         先跑：docker compose up -d redis postgres")
            return
        log("  依赖容器 redis / postgres 均在运行 ✓")

        if tcp_ok():
            log("  [!] 端口 %d 已被占用，先停掉占用进程再跑" % PORT)
            return
        log("  端口 %d 空闲，可以启动" % PORT)

        proc, tail = start_app()
        if not wait_ready(tail):
            log("  [!] 服务未就绪，中止")
            dump_tail(tail)
            return

        metrics_before = prometheus_counters()

        results = []
        r1, before, after = check_non_stream_finish_reason()
        results += r1
        results += check_stream_finish_frame()
        results += check_quota_settle()
        results += check_metrics(metrics_before or before, after)

        log()
        log("=" * 72)
        log("Step 6 · 汇总")
        log("=" * 72)
        ok = bool(results)
        for name, good, detail in results:
            log("  [%s] %-28s %s" % ("PASS" if good else "FAIL", name, detail))
            ok = ok and good
        if not ok:
            dump_tail(tail)
    finally:
        if proc and proc.poll() is None:
            log()
            log("Step 7 · 关闭服务")
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
            time.sleep(3)
            log("  已发送 taskkill /F /T，端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))
        log()
        log("=== 结果: %s ===" % ("F0 验证全部通过 ✅" if ok else "未通过 ❌（见上方 FAIL 项）"))
        flush()


main()
