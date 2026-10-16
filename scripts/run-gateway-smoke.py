# -*- coding: utf-8 -*-
"""
llm-gateway 真机联调脚本（自包含：起服务 → 等健康 → 打真实请求 → 收工）

之所以写成一个脚本而不是分步执行：服务是常驻进程，若跨轮次保持会不稳定。
本脚本把「起 - 测 - 停」放在一个进程里，跑完即退，只留一份日志。

用法::

    # 1) 密钥填进 config/application-local.yml（可从 .example 复制）
    # 2) 起依赖
    docker compose up -d redis postgres
    # 3) 跑联调（起服务 → 打 2 次真实请求 + 1 次流式 → 抓指标 → 关服务）
    python scripts/run-gateway-smoke.py

可覆盖的环境变量：GATEWAY_DIR / SMOKE_LOG / GATEWAY_PORT /
MVN_CMD / MAVEN_REPO / JAVA_HOME。
"""
import collections
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------- 路径与配置
# 默认值按「本脚本位于 <repo>/scripts/ 下」推导，clone 下来就能跑；
# 全部可用环境变量覆盖，适配不同的 JDK / Maven 安装位置。
ROOT = Path(os.environ.get("GATEWAY_DIR") or Path(__file__).resolve().parents[1])
LOG = Path(os.environ.get("SMOKE_LOG") or (ROOT / "out" / "run-gateway.log"))
PORT = int(os.environ.get("GATEWAY_PORT") or 8080)


def _pick_mvn() -> str:
    """按 环境变量 → 隔离安装 → PATH 的顺序挑 Maven。"""
    if os.environ.get("MVN_CMD"):
        return os.environ["MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo() -> str:
    """Maven 本地仓库：优先环境变量，其次本机隔离仓库，最后标准 ~/.m2。"""
    if os.environ.get("MAVEN_REPO"):
        return os.environ["MAVEN_REPO"]
    local = Path.home() / ".workbuddy/binaries/maven/repo"
    return str(local) if local.exists() else str(Path.home() / ".m2" / "repository")


MVN = _pick_mvn()
REPO = _pick_repo()
JAVA_HOME = os.environ.get("JAVA_HOME", "")   # 留空则沿用当前 shell 的 JAVA_HOME
MODEL_FLASH = "deepseek-flash"      # 实测可用：快、便宜
MODEL_PRO = "deepseek-v4-pro"       # 实测可用：强
PROMPT = "用一句话解释什么是语义缓存，不超过40字。"

lines = []


def log(msg=""):
    text = str(msg)
    lines.append(text)
    try:
        print(text, flush=True)
    except Exception:
        pass


def openai_fields(data, resp=None):
    """从 OpenAI 形状的响应里取出 (回答, 模型, usage, 缓存命中)。

    网关的成功响应已统一为 OpenAI 形状：
      choices[].message.content + usage.{prompt_tokens,completion_tokens,total_tokens}
    缓存命中走 X-Cache-Hit 响应头，不在 body 里。

    以前这里读的是扁平字段（content / promptTokens / cacheHit）。那些字段已经不存在，
    继续读不会报错、只会静默拿到 None —— 那是最难排查的一类回归，
    所以这个函数只认新形状，读不到就明确返回 None 而不是兜底成 0。
    """
    choices = data.get("choices") or []
    message = (choices[0].get("message") or {}) if choices else {}
    answer = message.get("content")
    usage = data.get("usage") or {}
    cache_hit = None
    if resp is not None:
        headers = {k.lower(): v for k, v in resp.getheaders()}
        cache_hit = headers.get("x-cache-hit")
    return answer, data.get("model"), usage, cache_hit


def flush():
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG, "w", encoding="utf-8", errors="replace") as f:
        f.write("\n".join(lines))


def mask(key):
    if not key:
        return "(空)"
    return key[:7] + "*" * 6 + " (len=%d)" % len(key)


# ---------------------------------------------------------------- 0. 前置检查
def check_config():
    path = os.path.join(ROOT, "config", "application-local.yml")
    raw = open(path, encoding="utf-8").read()
    m = re.search(r'api-key:\s*"([^"]*)"', raw)
    key = m.group(1) if m else ""
    log("=" * 72)
    log("Step 0 · 配置检查")
    log("=" * 72)
    log("  配置文件      : config/application-local.yml")
    log("  api-key       : %s" % mask(key))
    log("  base-url      : %s" % (re.search(r"base-url:\s*(\S+)", raw).group(1)))
    log("  models        : %s" % re.findall(r"-\s*(deepseek-\S+)", raw))
    log("  cache.enabled : %s" % re.search(r"enabled:\s*(\w+)", raw).group(1))
    if "粘贴" in key or "替换" in key or len(key) < 10:
        log("  [!] key 看起来还是占位符，中止")
        return False
    log("  [OK] key 已填写")
    return True


def port_busy():
    """端口是否已被监听。

    必须用原生 socket：环境里有 HTTP_PROXY/HTTPS_PROXY 指向宿主代理，
    urllib 默认走代理，探 127.0.0.1:8080 会拿到代理的 502 而不是"连接被拒"，
    从而把"服务还没起来"误判成"已就绪"。http.client / socket 是直连，不受代理影响。
    """
    return tcp_ok()


def tcp_ok(port=PORT, timeout=2):
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect(("127.0.0.1", port))
        return True
    except Exception:
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


# 显式禁用代理的 opener，用于访问本机 actuator（避免走宿主代理）
NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


# ---------------------------------------------------------------- 1. 启动服务
def start_app():
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"

    # 关键修复：宿主环境里有 SERVER__PORT=47480（WorkBuddy 自身内部服务的端口），
    # 会被 Spring Boot 的宽松绑定当成 server.port，导致 "Port 47480 was already in use"。
    # 双重保险：① 从子进程环境里摘掉；② 用命令行参数强制覆盖（优先级高于环境变量）。
    hijacked = {k: v for k, v in env.items() if k in ("SERVER__PORT", "SERVER_PORT", "SPRING_APPLICATION_JSON")}
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
    log("Step 1 · 启动服务（mvn spring-boot:run --spring.profiles.active=local）")
    log("=" * 72)
    if hijacked:
        log("  [修复] 检测到端口劫持环境变量，已从子进程摘除: %s" % hijacked)
    log("  [修复] 追加命令行参数强制端口: --server.port=%d" % PORT)
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


def wait_health(tail, timeout=300):
    """就绪判定：TCP 能连上 127.0.0.1:PORT 即视为服务已就绪。

    两个坑都踩过了：
      ① 不能要求健康状态是 UP —— Redis 没起时 /actuator/health 返回 503 DOWN；
      ② 不能用 urllib 探本机 —— 会走宿主 HTTP 代理，拿到代理的 502 假信号。
    所以以 socket 直连为准，health 仅用于补充信息。
    """
    t0 = time.time()
    deadline = t0 + timeout
    url = "http://127.0.0.1:%d/actuator/health" % PORT
    log("  等待 127.0.0.1:%d 开始监听（最长 %ds）..." % (PORT, timeout))
    last_tick = t0
    while time.time() < deadline:
        if any("APPLICATION FAILED TO START" in l or "BUILD FAILURE" in l for l in tail):
            return "failed", None
        if tcp_ok():
            elapsed = time.time() - t0
            log("  端口已监听（启动耗时约 %.0fs），探测 health..." % elapsed)
            time.sleep(1.5)
            try:
                r = NO_PROXY_OPENER.open(url, timeout=5)
                return "up", r.read().decode("utf-8", "replace")
            except urllib.error.HTTPError as he:
                return "up-degraded(HTTP %d)" % he.code, he.read().decode("utf-8", "replace")
            except Exception as e:
                return "up(health 探测异常: %s)" % e, None
        if time.time() - last_tick > 15:
            log("    ...编译/启动中（已 %.0fs）" % (time.time() - t0))
            last_tick = time.time()
        time.sleep(2)
    return "timeout", None


def clean(tail, n=40):
    """过滤掉堆栈行，只留有意义的信息行。"""
    skip = ("\tat ", "Caused by:", "... ", "Suppressed:")
    out = [l.strip() for l in tail if l.strip() and not l.strip().startswith(("\tat ", "Caused by:", "..."))]
    return out[-n:]


def started_line(tail):
    for l in tail:
        if "Started LlmGatewayApplication" in l or "Netty started on port" in l:
            return l.strip()
    return ""


def profile_line(tail):
    hits = [l.strip() for l in tail if "The following 1 profile is active" in l or "profile is active" in l]
    return hits[0] if hits else "(未找到 profile 激活日志)"


# ---------------------------------------------------------------- 2. 发请求
def post(path, payload, stream=False, tenant="tenant-a", timeout=180):
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    conn.request("POST", path, body=body, headers={
        "Content-Type": "application/json",
        "Accept": "text/event-stream" if stream else "application/json",
        "X-Api-Key": tenant,
    })
    resp = conn.getresponse()
    return conn, resp


def do_nonstream(model, step, note):
    log()
    log("=" * 72)
    log("Step %s · 非流式请求（model=%s）" % (step, model))
    log("=" * 72)
    log("  %s" % note)
    payload = {
        "model": model,
        "stream": False,
        "temperature": 0.2,
        "feature": "smoke-" + model,
        "messages": [{"role": "user", "content": PROMPT}],
    }
    log("  请求体: %s" % json.dumps(payload, ensure_ascii=False))
    t0 = time.time()
    try:
        conn, resp = post("/v1/chat/completions", payload)
        raw = resp.read().decode("utf-8", "replace")
        ms = (time.time() - t0) * 1000
        log("  HTTP 状态: %s" % resp.status)
        log("  响应头   : %s" % [h for h in resp.getheaders()
                                 if h[0].lower() in ("content-type", "x-trace-id",
                                                     "x-request-id", "x-cache-hit")])
        log("  耗时     : %.0f ms" % ms)
        try:
            data = json.loads(raw)
            if data.get("error"):
                log("  [上游返回错误] %s" % data.get("error"))
            answer, model, u, cache_hit = openai_fields(data, resp)
            log("  回答     : %s" % answer)
            log("  model    : %s" % model)
            log("  cacheHit : %s" % cache_hit)
            log("  usage    : %s" % u)
            pt = u.get("prompt_tokens") or 0
            ct = u.get("completion_tokens") or 0
            log("  => 账本按 gateway.pricing 单价计费（输入/输出分开）；"
                "查 Postgres: docker compose exec -T postgres psql -U postgres -d llmgw -c "
                "\"select trace_id,model,prompt_tokens,completion_tokens,cost from cost_ledger order by id desc limit 5\"")
            log("     本次 usage: prompt=%d completion=%d" % (pt, ct))
        except Exception as e:
            log("  [解析失败] %s" % e)
            log("  原始响应: %s" % raw[:1200])
        conn.close()
    except Exception as e:
        log("  [请求失败] %r" % e)


def do_stream():
    log()
    log("=" * 72)
    log("Step 4 · 流式请求（SSE，验证逐字吐 + 真实用量）")
    log("=" * 72)
    payload = {
        "model": MODEL_FLASH,
        "stream": True,
        "temperature": 0.2,
        "feature": "stream-smoke",
        # 显式索取用量帧：网关**总是**向上游要 usage（记账要用），
        # 但只在客户端开口时才转发给它。这里开口，是为了能当场验证
        # 流式账本用的是上游真实 token 而不是按字符数估算。
        "stream_options": {"include_usage": True},
        "messages": [{"role": "user", "content": "从1数到5，每个数字之间用空格分隔。"}],
    }
    log("  请求体: %s" % json.dumps(payload, ensure_ascii=False))
    t0 = time.time()
    try:
        conn, resp = post("/v1/chat/completions", payload)
        log("  HTTP 状态: %s" % resp.status)
        log("  响应头   : %s" % [h for h in resp.getheaders()
                                 if h[0].lower() in ("content-type", "x-accel-buffering")])
        chunks = []
        deltas = []
        seen_done = False
        first_at = None
        usage_frames = []
        chunk_models = set()
        for raw in resp:
            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            if not line:
                continue
            if first_at is None:
                first_at = time.time()
            if line.startswith("data: "):
                payload_text = line[6:]
                chunks.append(payload_text)
                # 流式也已经是 OpenAI chunk 信封了：增量在 choices[0].delta.content。
                # 终止帧是字面量 [DONE]，不是 JSON，必须先判掉再解析。
                if payload_text == "[DONE]":
                    seen_done = True
                    continue
                try:
                    node = json.loads(payload_text)
                    choices = node.get("choices") or []
                    delta = (choices[0].get("delta") or {}).get("content") if choices else None
                    # F5：分片里的 model 必须是**实际服务**的模型，不是请求里写的那个。
                    # 级联升级或缓存命中时两者不同，这里把出现过的模型名都收集起来。
                    if node.get("model"):
                        chunk_models.add(node["model"])
                    # 用量帧的形状是 choices 为空数组 + usage 非空。它没有 delta，
                    # 所以不算文本分片；误把它当分片会让"文本分片数"虚高。
                    if not choices and node.get("usage"):
                        usage_frames.append(node["usage"])
                except Exception:
                    delta = None
                if delta:
                    deltas.append(delta)
                if len(chunks) <= 6 or payload_text == "[DONE]":
                    log("    <- %s" % line)
        total_ms = (time.time() - t0) * 1000
        ttfb = (first_at - t0) * 1000 if first_at else -1
        log("  SSE 帧数 : %d（含终止帧）" % len(chunks))
        log("  文本分片 : %d" % len(deltas))
        log("  终止帧   : %s" % ("已收到 [DONE]" if seen_done else "!! 未收到 [DONE]"))
        log("  分片模型 : %s" % (", ".join(sorted(chunk_models)) or "!! 分片里没有 model 字段"))
        if usage_frames:
            u = usage_frames[-1]
            log("  真实用量 : prompt=%s completion=%s total=%s  ← 上游回传，非估算"
                % (u.get("prompt_tokens"), u.get("completion_tokens"), u.get("total_tokens")))
        else:
            log("  真实用量 : !! 未收到用量帧（账本会退回按字符数估算）")
        log("  首字延迟 : %.0f ms" % ttfb)
        log("  总耗时   : %.0f ms" % total_ms)
        if deltas:
            log("  拼接结果 : %s" % ("".join(deltas)[:150]))
        conn.close()
    except Exception as e:
        log("  [请求失败] %r" % e)


def do_health():
    log()
    log("=" * 72)
    log("Step 5 · 指标端点")
    log("=" * 72)
    for path in ("/actuator/health", "/actuator/prometheus"):
        try:
            try:
                r = NO_PROXY_OPENER.open("http://127.0.0.1:%d%s" % (PORT, path), timeout=5)
                code, body = r.status, r.read().decode("utf-8", "replace")
            except urllib.error.HTTPError as he:
                code, body = he.code, he.read().decode("utf-8", "replace")
            if path.endswith("prometheus"):
                hits = [l for l in body.splitlines() if l.startswith("llmgw_")]
                log("  %s -> HTTP %d，网关自定义指标 %d 条：" % (path, code, len(hits)))
                for h in hits[:10]:
                    log("      %s" % h)
                if not hits:
                    log("      （暂无数据：指标名为 llmgw.cache.hit / .miss / cascade.escalate）")
            else:
                log("  %s -> HTTP %d" % (path, code))
                log("      %s" % body.strip()[:400])
        except Exception as e:
            log("  %s -> 失败: %s" % (path, e))


# ---------------------------------------------------------------- main
def main():
    ok = False
    proc = None
    tail = None
    try:
        if not check_config():
            return
        if port_busy():
            log()
            log("[!] 端口 %d 已被占用（可能是上次残留的服务）。请先关掉再跑。" % PORT)
            return
        log()
        log("  端口 %d 空闲，可以启动" % PORT)

        proc, tail = start_app()
        state, health_body = wait_health(tail)
        log("  就绪判定: %s" % state)
        log("  profile 日志: %s" % profile_line(tail))
        if state.startswith("up"):
            log("  启动日志: %s" % started_line(tail))
            log("  健康响应: %s" % (health_body or "")[:300])
            do_nonstream(MODEL_FLASH, "2", "先用最快最便宜的档位探路")
            do_nonstream(MODEL_PRO, "3", "同一问题换强模型，验证网关可切换上游模型")
            do_stream()
            do_health()
            ok = True
        else:
            log()
            log("--- 应用输出（已滤除堆栈，诊断用）---")
            for l in clean(tail, 40):
                log("  | " + l)
    except Exception as e:
        import traceback
        log("[异常] %s" % e)
        log(traceback.format_exc())
        if tail:
            log("--- 输出尾部 ---")
            for l in list(tail)[-40:]:
                log("  | " + l)
    finally:
        if proc and proc.poll() is None:
            log()
            log("Step 6 · 关闭服务")
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True)
            time.sleep(3)
            log("  已发送 taskkill /F /T，端口占用: %s" % ("是" if port_busy() else "否（已释放）"))
        if tail:
            log()
            log("--- 应用日志尾部（已滤除堆栈，最后 20 行）---")
            for l in clean(tail, 20):
                log("  | " + l)
        log()
        log("=== 结果: %s ===" % ("链路打通 ✅" if ok else "未跑通 ❌（见上方诊断）"))
        flush()


main()
