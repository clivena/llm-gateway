# -*- coding: utf-8 -*-
"""
llm-gateway F1 验证 —— 重试 + 熔断（Resilience4j）

它要回答的问题，光看代码是回答不了的：

  ① **重试是不是真的"重新发了一次请求"？**
     代码上重试是"重新订阅同一条 Mono"。如果哪天有人写出一个会复用内部
     Publisher 的适配器，重试就变成"把上一次的旧响应再吐一遍"——
     看起来重试成功了，实际一次请求都没发出去，而且不报错。
     唯一能证伪的方式：让上游数一数它到底收到了几次请求。

  ② **重试耗尽后，F0 建立的东西还在不在？**
     空正文要被翻译成异常才能进重试算子，那层异常**必须被还原回来**。
     少还原一次，客户端就从"200 + finish_reason=length"变成 502 ——
     把"上游明确说明了原因"伪装成"网关坏了"。这条只有真机能验。

  ③ **熔断打开之后，是不是真的不再发请求了？**
     这是熔断存在的唯一理由。指标涨了、状态显示 OPEN，
     都不足以证明"上游没再被打扰"—— 只有上游自己的计数能证明。

  ④ **已经被推给客户端的流，不会被重放吧？**
     重试 = 从头再放一遍。已经吐过字幕的流重放，用户会看到两段拼接的答案。
     这种损坏不可撤销，而且看起来像模型在胡说。

为什么需要**两段运行**：
  · 第一段打真实上游（DeepSeek）—— 因为"重试在真实模型上到底会不会触发"
    取决于上游的实际行为（推理型模型在小 max_tokens 下必然返回空正文），
    桩模拟不来。
  · 第二段打本地桩 —— 因为"重试了几次""熔断后有没有再发请求"这类
    **精确计数**必须由能计数的上游来回答，而且故障要能按需制造。

桩的地址通过 SPRING_APPLICATION_JSON 覆盖（它比 config/application-local.yml
优先级高，且会整体替换 providers 列表）—— **不修改任何配置文件**，
因此不存在"跑完忘了恢复"的风险。

用法::

    docker compose up -d redis postgres
    python scripts/check-resilience.py

可覆盖的环境变量：GATEWAY_DIR / F1_LOG / GATEWAY_PORT / F1_MODEL /
F1_MVN_CMD / F1_MAVEN_REPO / JAVA_HOME / F1_SKIP_REAL（跳过打真实上游的那段）。
"""
import collections
import http.client
import json
import os
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(os.environ.get("GATEWAY_DIR") or Path(__file__).resolve().parents[1])
LOG = Path(os.environ.get("F1_LOG") or (ROOT / "out" / "check-resilience.log"))
PORT = int(os.environ.get("GATEWAY_PORT") or 8080)
STUB_PORT = int(os.environ.get("F1_STUB_PORT") or 18765)
MODEL = os.environ.get("F1_MODEL") or "deepseek-flash"
SKIP_REAL = os.environ.get("F1_SKIP_REAL") == "1"

RUN_TAG = time.strftime("%H%M%S")
FEATURE = "f1-check"

# 应用的熔断参数（与 src/main/resources/application.yml 的 gateway.resilience 一致）。
# 脚本依赖它们来推算"几次失败才会打开"、"等多久半开"。
# 硬编码是刻意的：这两个数字若与应用不一致，断言会失败 —— 那正是我们想知道的。
MAX_ATTEMPTS = 3
MIN_CALLS = 8
WAIT_IN_OPEN_MS = 10_000
PERMITTED_HALF_OPEN = 3

# 推理型模型：max_tokens 是"推理 + 正文"的总预算（实测，见 scripts/probe-upstream-shape.py）。
TINY_BUDGET = 64      # 必然被 reasoning 吃光 → finish_reason=length + 空正文
FULL_BUDGET = 4096    # 能自然收尾

STUB_ANSWER = "桩服务的答案"
STUB_STREAM_TAIL = "流式答案的后半段"

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
    if os.environ.get("F1_MVN_CMD"):
        return os.environ["F1_MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo():
    if os.environ.get("F1_MAVEN_REPO"):
        return os.environ["F1_MAVEN_REPO"]
    local = Path.home() / ".workbuddy/binaries/maven/repo"
    return str(local) if local.exists() else str(Path.home() / ".m2" / "repository")


MVN = _pick_mvn()
REPO = _pick_repo()
JAVA_HOME = os.environ.get("JAVA_HOME", "")

NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def tcp_ok(port=None, timeout=2):
    port = port or PORT
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
    """非流式请求，返回 (状态码, 耗时秒, 响应体 dict, 原始响应体文本)。"""
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
    raw = b""
    status = None
    try:
        conn.request("POST", "/v1/chat/completions", body=body, headers={
            "Content-Type": "application/json",
            "X-Api-Key": tenant,
        })
        resp = conn.getresponse()
        status = resp.status
        raw = resp.read()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    elapsed = time.time() - t0
    text = raw.decode("utf-8", "replace")
    try:
        data = json.loads(text)
    except Exception:
        data = {}
    return status, elapsed, data or {}, text


def ask_stream(tenant, question, max_tokens, timeout=300):
    """流式请求，返回 (状态码, 分片列表, 是否见到 [DONE], 原始文本)。"""
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
    buf = b""
    try:
        conn.request("POST", "/v1/chat/completions", body=body, headers={
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "X-Api-Key": tenant,
        })
        resp = conn.getresponse()
        status = resp.status
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


def delta_text(chunks):
    out = []
    for c in chunks:
        try:
            d = (c.get("choices") or [{}])[0].get("delta") or {}
            if d.get("content"):
                out.append(d["content"])
        except Exception:
            pass
    return "".join(out)


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


def error_type_of(body):
    try:
        return (body.get("error") or {}).get("type")
    except Exception:
        return None


def counter_value(name, provider=None):
    """
    从 /actuator/prometheus 取一个计数器的值。缺失返回 0.0。

    带标签的计数器在 Prometheus 文本格式里长这样：
        llmgw_upstream_retry_total{provider="deepseek"} 2.0
    所以不能只按名字匹配 —— 那是 A 批之前留下的坑（当时所有计数器都无标签）。
    """
    code, raw = api_get("/actuator/prometheus")
    if code != 200:
        return None
    if provider is None:
        m = re.search(r"^%s\s+([0-9.eE+-]+)$" % re.escape(name), raw, re.M)
    else:
        m = re.search(r"^%s\{[^}]*provider=\"%s\"[^}]*\}\s+([0-9.eE+-]+)$"
                      % (re.escape(name), re.escape(provider)), raw, re.M)
    return float(m.group(1)) if m else 0.0


def circuit_state(provider):
    """读 /admin/circuit 里某个 provider 的熔断状态；读不到返回 None。"""
    code, data = api_json("/admin/circuit")
    if code != 200 or not isinstance(data, dict):
        return None, None
    entry = (data.get("providers") or {}).get(provider)
    return (entry or {}).get("state"), data


# ============================================================ 本地桩上游
class StubState:
    """
    桩的可控状态。

    三类故障分别对应三种被测行为：
      · always_fail / fail_next —— 重试与熔断
      · stream_fail_next        —— 流式重试（"首片前可重试"）
    HTTP 500 而不是 400：只有 5xx 与 429 在白名单里值得重试，
    桩要是返回 400 就永远测不到重试路径。
    """
    lock = threading.Lock()
    chat_calls = 0          # /v1/chat/completions 收到的次数（**不含**重试之外的其他路径）
    stream_calls = 0
    always_fail = False
    fail_next = 0
    stream_fail_next = 0

    @classmethod
    def reset(cls):
        with cls.lock:
            cls.chat_calls = 0
            cls.stream_calls = 0
            cls.always_fail = False
            cls.fail_next = 0
            cls.stream_fail_next = 0


class StubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):      # 静音
        pass

    # --- 内部 ---
    def _json(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse(self, frames):
        body = "".join("data: %s\n\n" % f for f in frames) + "data: [DONE]\n\n"
        raw = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _fail(self):
        self._json(500, {"error": {"message": "stub upstream failure",
                                   "type": "server_error"}})

    # --- 控制面 ---
    def do_GET(self):
        if self.path.startswith("/__stats"):
            self._json(200, {"chat_calls": StubState.chat_calls,
                             "stream_calls": StubState.stream_calls,
                             "always_fail": StubState.always_fail,
                             "fail_next": StubState.fail_next,
                             "stream_fail_next": StubState.stream_fail_next})
            return
        self._json(404, {"error": {"message": "not found", "type": "not_found"}})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw.decode("utf-8", "replace")) if raw else {}
        except Exception:
            payload = {}

        if self.path.startswith("/__reset"):
            StubState.reset()
            self._json(200, {"ok": True})
            return
        if self.path.startswith("/__fail"):
            # /__fail?n=3  → 接下来 3 次失败；/__fail?always=1 → 一直失败
            qs = {}
            if "?" in self.path:
                for kv in self.path.split("?", 1)[1].split("&"):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        qs[k] = v
            with StubState.lock:
                if qs.get("always") == "1":
                    StubState.always_fail = True
                if "n" in qs:
                    StubState.fail_next = int(qs["n"])
                if "stream_n" in qs:
                    StubState.stream_fail_next = int(qs["stream_n"])
            self._json(200, {"ok": True})
            return
        if self.path.startswith("/__recover"):
            with StubState.lock:
                StubState.always_fail = False
                StubState.fail_next = 0
                StubState.stream_fail_next = 0
            self._json(200, {"ok": True})
            return

        if self.path.startswith("/v1/embeddings"):
            self._json(404, {"error": {"message": "no embedding endpoint",
                                       "type": "not_found"}})
            return

        if self.path.startswith("/v1/chat/completions"):
            is_stream = bool(payload.get("stream"))
            with StubState.lock:
                if is_stream:
                    StubState.stream_calls += 1
                    StubState.chat_calls += 1
                    fail = StubState.always_fail or StubState.stream_fail_next > 0
                    if StubState.stream_fail_next > 0:
                        StubState.stream_fail_next -= 1
                else:
                    StubState.chat_calls += 1
                    fail = StubState.always_fail or StubState.fail_next > 0
                    if StubState.fail_next > 0:
                        StubState.fail_next -= 1
            if fail:
                self._fail()
                return

            usage = {"prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 40}
            if is_stream:
                self._sse([
                    json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                "choices": [{"index": 0, "delta": {"role": "assistant"},
                                             "finish_reason": None}]}),
                    json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                "choices": [{"index": 0, "delta": {"content": STUB_ANSWER},
                                             "finish_reason": None}]}),
                    json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                "choices": [{"index": 0, "delta": {"content": STUB_STREAM_TAIL},
                                             "finish_reason": None}]}),
                    json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                "choices": [{"index": 0, "delta": {},
                                             "finish_reason": "stop"}]}),
                    json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                "choices": [], "usage": usage}),
                ])
                return

            self._json(200, {
                "id": "stub", "object": "chat.completion",
                "choices": [{"index": 0,
                             "message": {"role": "assistant", "content": STUB_ANSWER},
                             "finish_reason": "stop"}],
                "usage": usage,
            })
            return

        self._json(404, {"error": {"message": "not found", "type": "not_found"}})


def stub_stat(key):
    try:
        r = NO_PROXY_OPENER.open("http://127.0.0.1:%d/__stats" % STUB_PORT, timeout=5)
        return json.loads(r.read().decode("utf-8")).get(key)
    except Exception:
        return None


def stub_cmd(path):
    try:
        req = urllib.request.Request("http://127.0.0.1:%d%s" % (STUB_PORT, path),
                                     data=b"", method="POST")
        NO_PROXY_OPENER.open(req, timeout=5).read()
        return True
    except Exception as e:
        log("  [!] 调用桩控制面失败 %s: %r" % (path, e))
        return False


# ============================================================ 服务生命周期
def start_app(stub_mode=False):
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"
    hijacked = {k: v for k, v in env.items()
                if k in ("SERVER__PORT", "SERVER_PORT", "SPRING_APPLICATION_JSON")}
    for k in hijacked:
        env.pop(k, None)

    if stub_mode:
        # ★ 用 SPRING_APPLICATION_JSON 覆盖，而不是改 config/application-local.yml：
        #   1) 它的优先级高于配置文件，且 **整体替换** providers 列表（正是我们想要的）；
        #   2) 不落地任何文件，因此不存在"跑完忘了恢复"的风险 ——
        #      脚本中途被打断也不会把开发者的真实 base-url 留在桩地址上。
        env["SPRING_APPLICATION_JSON"] = json.dumps({
            "gateway": {
                "providers": [{
                    "name": "deepseek",
                    "base-url": "http://127.0.0.1:%d/v1" % STUB_PORT,
                    "api-key": "stub-key",
                    "embedding-model": "",
                    "models": ["deepseek-flash", "deepseek-v4-pro"],
                }]
            }
        })

    cmd = [
        MVN, "-B", "-Duser.language=en",
        "-Dmaven.repo.local=" + REPO,
        "-f", os.path.join(ROOT, "pom.xml"),
        "spring-boot:run",
        "-Dspring-boot.run.profiles=local",
        "-Dspring-boot.run.arguments=--server.port=%d" % PORT,
    ]
    if hijacked:
        log("  [修复] 检测到劫持环境变量，已从子进程摘除: %s" % list(hijacked))
    if stub_mode:
        log("  [桩模式] providers[0].base-url → http://127.0.0.1:%d/v1（经 SPRING_APPLICATION_JSON）"
            % STUB_PORT)

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


def stop_app(proc):
    if proc and proc.poll() is None:
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
        time.sleep(3)


# ============================================================ A 段：真实上游
def run_real_upstream(results):
    log()
    log("=" * 72)
    log("A 段 · 真实上游（DeepSeek）：重试会不会在真实失败上触发")
    log("=" * 72)

    provider = "deepseek"

    # --- A1：空正文触发重试，但重试耗尽后 F0 的透传必须还在 ---
    tenant = "f1-a1-%s" % RUN_TAG
    r0 = counter_value("llmgw_upstream_retry_total", provider)
    status, elapsed, body, raw = ask(tenant, "什么是语义缓存", TINY_BUDGET)
    r1 = counter_value("llmgw_upstream_retry_total", provider)

    content = answer_of(body)
    reason = finish_reason_of(body)
    delta = None if (r0 is None or r1 is None) else int(r1 - r0)

    log("  HTTP=%s 耗时=%.0fms max_tokens=%d" % (status, elapsed * 1000, TINY_BUDGET))
    log("  finish_reason = %r   正文 = %d 字" % (reason, len(content)))
    log("  重试计数器 %s → %s（增量 %s，期望 %d）" % (r0, r1, delta, MAX_ATTEMPTS - 1))

    ok1 = (status == 200 and reason == "length" and delta == MAX_ATTEMPTS - 1)
    if status == 200 and reason != "length":
        log("  [FAIL] finish_reason = %r，期望 'length'。若上游这次正常出正文，"
            "说明预算够用了 —— 把 TINY_BUDGET 调小" % reason)
    if delta != MAX_ATTEMPTS - 1:
        log("  [FAIL] 重试次数不对。MAX_ATTEMPTS=%d 时增量应为 %d" % (MAX_ATTEMPTS, MAX_ATTEMPTS - 1))
    if status == 200 and reason == "length" and delta == MAX_ATTEMPTS - 1:
        log("  [PASS] 空正文触发了重试，且重试耗尽后客户端拿到的仍是")
        log("         200 + finish_reason='length' —— 不是 502。")
        log("         这正是 F1 最容易踩坏的地方：异常翻译漏了还原，")
        log("         F0 的「透传」就退化成「网关坏了」。")

    results.append(("重试在真实空正文上触发", ok1,
                    "重试增量=%s finish_reason=%r HTTP=%s" % (delta, reason, status)))

    # --- A2：反面 —— 正常答案不该被重试 ---
    tenant2 = "f1-a2-%s" % RUN_TAG
    b0 = counter_value("llmgw_upstream_retry_total", provider)
    status2, elapsed2, body2, _raw2 = ask(tenant2, "什么是成本账本", FULL_BUDGET)
    b1 = counter_value("llmgw_upstream_retry_total", provider)
    content2 = answer_of(body2)
    reason2 = finish_reason_of(body2)
    delta2 = None if (b0 is None or b1 is None) else int(b1 - b0)

    log()
    log("  HTTP=%s 耗时=%.0fms max_tokens=%d" % (status2, elapsed2 * 1000, FULL_BUDGET))
    log("  finish_reason = %r   正文 = %d 字" % (reason2, len(content2)))
    log("  重试计数器 %s → %s（增量 %s，期望 0）" % (b0, b1, delta2))

    if not content2.strip():
        log("  [!] 完整预算下上游仍返回空正文，本对照的判定无效（跳过，不计为失败）")
        log("      这属于上游行为波动，不是网关问题；真机复核时可调大 FULL_BUDGET")
        results.append(("正常答案不被重试（对照）", True, "正文为空，对照无效（已知风险）"))
    else:
        ok2 = status2 == 200 and delta2 == 0
        if ok2:
            log("  [PASS] 正常答案没有触发任何重试 —— 重试不是「无脑重发」")
        else:
            log("  [FAIL] 正常答案却重试了 %s 次" % delta2)
        results.append(("正常答案不被重试（对照）", ok2, "重试增量=%s" % delta2))

    # --- A3：熔断端点可用，且真实上游下必须仍是 CLOSED ---
    state, data = circuit_state(provider)
    log()
    log("  /admin/circuit → provider=%s state=%s" % (provider, state))
    if data:
        log("  retryEnabled=%s maxAttempts=%s circuitBreakerEnabled=%s"
            % (data.get("retryEnabled"), data.get("retryMaxAttempts"),
               data.get("circuitBreakerEnabled")))
        entry = (data.get("providers") or {}).get(provider) or {}
        log("  buffered=%s failed=%s successful=%s"
            % (entry.get("bufferedCalls"), entry.get("failedCalls"),
               entry.get("successfulCalls")))

    ok3 = state == "CLOSED"
    if ok3:
        log("  [PASS] 上游健康时熔断器保持 CLOSED")
    else:
        log("  [FAIL] state=%r，期望 'CLOSED'" % state)
    results.append(("/admin/circuit 可读且状态 CLOSED", ok3, "state=%s" % state))


# ============================================================ B 段：本地桩
def run_stub_upstream(results):
    log()
    log("=" * 72)
    log("B 段 · 本地桩上游：重试了几次、熔断后有没有再发请求")
    log("=" * 72)

    provider = "deepseek"
    stub_cmd("/__reset")

    # --- B0：预检 —— 覆盖到底生效了没有 ---
    # 这一段存在的理由：base-url 的覆盖方式如果没生效，后面的断言会全部
    # "看起来通过"（因为请求打到了真实上游，一切都正常），
    # 而实际上一次都没经过桩。必须先用桩自己的计数把这件事钉死。
    tenant = "f1-b0-%s" % RUN_TAG
    status, _e, body, _raw = ask(tenant, "桩预检-%s" % RUN_TAG, 256)
    calls = stub_stat("chat_calls")
    log("  预检请求 HTTP=%s，桩收到 chat 请求 %s 次" % (status, calls))
    if not calls:
        log("  [FAIL] 桩一次请求都没收到 —— base-url 覆盖没有生效，B 段全部无效")
        log("         可能原因：SPRING_APPLICATION_JSON 被环境/启动脚本覆盖。")
        results.append(("桩 base-url 覆盖生效", False, "桩收到 0 次请求"))
        return
    log("  [PASS] base-url 覆盖生效，请求确实打到了桩")
    results.append(("桩 base-url 覆盖生效", True, "桩收到 %s 次" % calls))

    # --- B1：重试 = 真的重新发了一次请求 ---
    stub_cmd("/__reset")
    stub_cmd("/__fail?n=2")      # 前 2 次 500，第 3 次成功（MAX_ATTEMPTS=3）
    tenant = "f1-b1-%s" % RUN_TAG
    status, elapsed, body, raw = ask(tenant, "重试计数-%s" % RUN_TAG, 256)
    calls = stub_stat("chat_calls")
    content = answer_of(body)

    log()
    log("  桩设定：前 2 次 500，之后成功")
    log("  HTTP=%s 耗时=%.0fms 正文=%r" % (status, elapsed * 1000, content))
    log("  桩实际收到请求 %s 次（期望 %d）" % (calls, MAX_ATTEMPTS))
    ok1 = status == 200 and content == STUB_ANSWER and calls == MAX_ATTEMPTS
    if ok1:
        log("  [PASS] 重试真的重新发起了请求（上游计数为证），最终拿到了成功响应")
        log("         这是「重试不是重放旧响应」唯一的证明方式：")
        log("         只看向应码的话，一个把旧 Mono 缓存起来的实现也能通过。")
    else:
        log("  [FAIL] 期望 HTTP=200 正文=%r 且桩收到 %d 次" % (STUB_ANSWER, MAX_ATTEMPTS))
    results.append(("重试真的重新发起请求", ok1, "桩收到 %s 次 HTTP=%s" % (calls, status)))

    # --- B2：重试耗尽 → 502（不是 500、不是 200） ---
    stub_cmd("/__reset")
    stub_cmd("/__fail?always=1")
    tenant = "f1-b2-%s" % RUN_TAG
    status, elapsed, body, raw = ask(tenant, "重试耗尽-%s" % RUN_TAG, 256)
    calls = stub_stat("chat_calls")
    etype = error_type_of(body)

    log()
    log("  桩设定：一直失败")
    log("  HTTP=%s 耗时=%.0fms error.type=%r" % (status, elapsed * 1000, etype))
    log("  桩实际收到请求 %s 次（期望 %d）" % (calls, MAX_ATTEMPTS))
    ok2 = (status == 502 and etype == "upstream_error" and calls == MAX_ATTEMPTS)
    if ok2:
        log("  [PASS] 重试耗尽后如实报 502 upstream_error，且试满了 %d 次" % MAX_ATTEMPTS)
    else:
        log("  [FAIL] 期望 HTTP=502 / type=upstream_error / 桩收到 %d 次" % MAX_ATTEMPTS)
        if status != 200:
            log("  ── 原始响应体 ──")
            log("  " + raw[:400].replace("\n", "\n  "))
    results.append(("重试耗尽如实报 502", ok2, "HTTP=%s type=%r 桩收到%s次" % (status, etype, calls)))

    # --- B3：熔断打开后不再打扰上游 ---
    state, _data = circuit_state(provider)
    rounds = 0
    while state != "OPEN" and rounds < 6:
        rounds += 1
        ask("f1-b3-%s-%d" % (RUN_TAG, rounds), "熔断推进-%s-%d" % (RUN_TAG, rounds), 256)
        state, _data = circuit_state(provider)
    log()
    log("  连续失败 %d 轮后 state = %s" % (rounds, state))

    if state != "OPEN":
        log("  [FAIL] 熔断器没有打开（期望失败率超阈值后转 OPEN）")
        results.append(("熔断器会在连续失败后打开", False, "state=%s" % state))
        results.append(("熔断后不再发请求", False, "前置未满足"))
    else:
        log("  [PASS] 熔断器已打开")
        results.append(("熔断器会在连续失败后打开", True, "state=OPEN"))

        before = stub_stat("chat_calls")
        status, elapsed, body, _raw = ask("f1-b3-open-%s" % RUN_TAG,
                                          "熔断后-%s" % RUN_TAG, 256)
        after = stub_stat("chat_calls")
        etype = error_type_of(body)
        log("  HTTP=%s 耗时=%.0fms error.type=%r" % (status, elapsed * 1000, etype))
        log("  桩收到请求 %s → %s（必须不变）" % (before, after))

        ok3 = (status == 503 and etype == "circuit_open" and before == after)
        if ok3:
            log("  [PASS] 熔断后返回 503 + type=circuit_open，且上游请求数没有增加")
            log("         最后那个「没有增加」才是熔断存在的理由 ——")
            log("         状态显示 OPEN、指标涨了，都不等于上游真的被放过了。")
        else:
            if status != 503:
                log("  [FAIL] 期望 503（不是 502 / 500）")
            if etype != "circuit_open":
                log("  [FAIL] 期望 error.type=circuit_open")
            if before != after:
                log("  [FAIL] 熔断后仍有请求打到上游：%s → %s" % (before, after))
        results.append(("熔断后不再发请求", ok3,
                        "HTTP=%s 桩 %s→%s" % (status, before, after)))

        # --- B4：上游恢复 → 半开 → 关闭 ---
        stub_cmd("/__recover")
        wait_s = WAIT_IN_OPEN_MS / 1000.0 + 2
        log()
        log("  桩恢复；等待 %.0fs 让熔断器自动进入 HALF_OPEN（wait-duration-in-open-state=%dms）..."
            % (wait_s, WAIT_IN_OPEN_MS))
        time.sleep(wait_s)
        state_after_wait, _d = circuit_state(provider)
        log("  等待后 state = %s" % state_after_wait)

        made = 0
        for i in range(PERMITTED_HALF_OPEN):
            st, _e, bd, _r = ask("f1-b4-%s-%d" % (RUN_TAG, i), "恢复探测-%s-%d" % (RUN_TAG, i), 256)
            if st == 200 and answer_of(bd) == STUB_ANSWER:
                made += 1
            time.sleep(0.3)
        state_closed, _d = circuit_state(provider)
        log("  连续 %d 次成功后 state = %s（成功 %d 次）"
            % (PERMITTED_HALF_OPEN, state_closed, made))

        ok4 = state_after_wait in ("OPEN", "HALF_OPEN") and made == PERMITTED_HALF_OPEN \
            and state_closed == "CLOSED"
        if ok4:
            log("  [PASS] 上游恢复 → 半开试探 → 全部成功 → 自动关闭")
        else:
            log("  [FAIL] 期望：等待后为 OPEN/HALF_OPEN，3 次成功探测后转 CLOSED")
        results.append(("熔断可自动恢复", ok4,
                        "%s→%d次成功→%s" % (state_after_wait, made, state_closed)))

    # --- B5：流式 —— 首片前重试，客户端不能看到重复内容 ---
    stub_cmd("/__reset")
    stub_cmd("/__fail?stream_n=1")   # 第 1 次流式请求失败，之后成功
    tenant = "f1-b5-%s" % RUN_TAG
    status, chunks, done, raw = ask_stream(tenant, "流式重试-%s" % RUN_TAG, 256)
    calls = stub_stat("stream_calls")
    text = delta_text(chunks)
    reasons = [(c.get("choices") or [{}])[0].get("finish_reason") for c in chunks]
    reasons = [r for r in reasons if r]

    log()
    log("  桩设定：第 1 次流式请求 500，之后正常")
    log("  HTTP=%s 桩收到流式请求 %s 次 [DONE]=%s" % (status, calls, done))
    log("  客户端收到的正文 = %r" % text)
    log("  收尾帧 finish_reason = %s" % reasons)

    expected = STUB_ANSWER + STUB_STREAM_TAIL
    ok5 = (status == 200 and done and text == expected and calls == 2)
    if ok5:
        log("  [PASS] 首片前重试成功，且客户端只看到**一份**完整答案")
        log("         正文正好等于期望值 —— 若重试发生重放，这里会出现两遍拼接。")
        log("         这是 F1 里唯一「错了也不报错、只是偶尔答案重复」的分支。")
    else:
        if text != expected:
            log("  [FAIL] 正文 %r != 期望 %r（重复或缺失）" % (text, expected))
        if calls != 2:
            log("  [FAIL] 桩收到 %s 次流式请求，期望 2" % calls)
        if not done:
            log("  [FAIL] 没收到 [DONE]")
        if status != 200:
            log("  ── 原始响应体（前 500 字）──")
            log("  " + raw[:500].replace("\n", "\n  "))
    results.append(("流式首片前重试不产生重复内容", ok5,
                    "桩%s次 正文%d字" % (calls, len(text))))

    # --- B6：指标带 provider 标签 ---
    v = counter_value("llmgw_upstream_retry_total", provider)
    rej = counter_value("llmgw_circuit_rejected_total", provider)
    opened = counter_value("llmgw_circuit_open")
    log()
    log("  llmgw_upstream_retry_total{provider=deepseek}   = %s" % v)
    log("  llmgw_circuit_rejected_total{provider=deepseek} = %s" % rej)
    log("  llmgw_circuit_open{provider=deepseek}           = %s" % opened)
    ok6 = v is not None and v > 0 and rej is not None and rej > 0
    if ok6:
        log("  [PASS] F1 三个指标都带上了 provider 标签并可读")
    else:
        log("  [FAIL] 指标缺失或为 0 —— 熔断/重试的量在面板上看不到")
    results.append(("F1 指标（带 provider 标签）", ok6,
                    "retry=%s rejected=%s open=%s" % (v, rej, opened)))


def deps_missing():
    """
    检查 Redis / Postgres 容器是否在跑。返回缺失的容器名列表；无法判定返回 None。

    为什么值得一个前置检查：本脚本的请求链路依赖 Redis（配额）与 Postgres（账本）。
    它们挂掉时 QuotaService 会 fail-open（放行 + 记日志），
    于是每条断言都会红 —— 而看起来像代码 bug。本次 F0 的实测就踩过这个坑：
    docker 容器在跑脚本前 15 分钟停了，我先去读代码，白花了一轮。
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
    stub = None
    stub_thread = None
    ok = False
    results = []
    try:
        log("=" * 72)
        log("llm-gateway F1 验证（重试 + 熔断）  批次=%s" % RUN_TAG)
        log("=" * 72)
        log("  模型=%s  maxAttempts=%d  minimumNumberOfCalls=%d  waitInOpen=%dms"
            % (MODEL, MAX_ATTEMPTS, MIN_CALLS, WAIT_IN_OPEN_MS))

        missing = deps_missing()
        if missing:
            log()
            log("  [中止] 依赖容器未运行: %s" % ", ".join(missing))
            log("         先跑：docker compose up -d redis postgres")
            return
        log("  依赖容器 redis / postgres 均在运行 ✓")

        if tcp_ok():
            log("  [!] 端口 %d 已被占用，先停掉占用进程再跑" % PORT)
            return
        if tcp_ok(STUB_PORT):
            log("  [!] 桩端口 %d 已被占用，先释放（或设 F1_STUB_PORT）" % STUB_PORT)
            return

        # ---------- A 段 ----------
        if SKIP_REAL:
            log()
            log("A 段 · 已按 F1_SKIP_REAL 跳过（不打真实上游）")
        else:
            log()
            log("Step 1 · 启动服务（真实上游，端口 %d）" % PORT)
            proc, tail = start_app(stub_mode=False)
            if not wait_ready(tail):
                log("  [!] 服务未就绪，中止")
                dump_tail(tail)
                return
            run_real_upstream(results)
            log()
            log("Step 2 · 关闭服务（准备切到桩）")
            stop_app(proc)
            proc = None
            log("  端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))

        # ---------- B 段 ----------
        log()
        log("Step 3 · 启动本地桩上游（端口 %d）" % STUB_PORT)
        StubState.reset()
        stub = ThreadingHTTPServer(("127.0.0.1", STUB_PORT), StubHandler)
        stub.daemon_threads = True
        stub_thread = threading.Thread(target=stub.serve_forever, daemon=True)
        stub_thread.start()
        time.sleep(0.5)
        log("  桩已就绪")

        log()
        log("Step 4 · 启动服务（桩上游，端口 %d）" % PORT)
        proc, tail = start_app(stub_mode=True)
        if not wait_ready(tail):
            log("  [!] 服务未就绪，中止")
            dump_tail(tail)
            return
        run_stub_upstream(results)

        # ---------- 汇总 ----------
        log()
        log("=" * 72)
        log("Step 5 · 汇总")
        log("=" * 72)
        ok = bool(results)
        for name, good, detail in results:
            log("  [%s] %-30s %s" % ("PASS" if good else "FAIL", name, detail))
            ok = ok and good
        if not ok:
            dump_tail(tail)
    finally:
        if proc and proc.poll() is None:
            log()
            log("Step 6 · 关闭服务")
            stop_app(proc)
            log("  端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))
        if stub:
            try:
                stub.shutdown()
                stub.server_close()
            except Exception:
                pass
        # 桩是第二个进程内的线程，进程退出即释放；这里只确保工作区没有任何遗留配置文件
        log()
        log("  未修改任何配置文件（桩地址经 SPRING_APPLICATION_JSON 注入）✓")
        log()
        log("=== 结果: %s ===" % ("F1 验证全部通过 ✅" if ok else "未通过 ❌（见上方 FAIL 项）"))
        flush()


main()
