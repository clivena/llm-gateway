# -*- coding: utf-8 -*-
"""
llm-gateway F2 验证 —— 跨 provider 故障转移

它要回答的问题，光看代码同样回答不了：

  ① **"转移"到底是真换了一家用，还是又打了同一家一次？**
     单元测试用的是 mock 适配器，"换了"只是调用计数变了。
     真机要问的是：**第二家上游自己收到了几次请求**。
     只有上游的计数能回答这个 —— 指标涨了、日志打了"failing over"都不算。

  ② **候选的声明顺序是不是真的等于优先级？**
     顺序在配置里读起来是"谁优先"，但代码完全可能把候选遍历一遍再挑
     （比如按健康度排序）。症状会是"坏候选在链尾也有代价"，
     而这一点只能靠**延迟**测出来：链首不可达时，每次请求都要先在那家
     把重试预算烧完，才轮到下一家。

  ③ **熔断之后，坏掉的那家还会不会被敲？**
     F1 已经证明"熔断打开就不再发请求"。F2 要证明的是更强的一句：
     熔断打开的那家在**故障转移路径**上也要被跳过 —— 而不是
     "先试着调用一下、被拒了再换家"。后者的代价是备选只剩 1 次重试机会。

  ④ **账单会不会被乘法放大？**
     max-attempts(3) × 候选家数(2) = 6。上限必须落在**请求级**：
     两家各打满重试预算，与"总共只发 4 次"是两种完全不同的故障期成本。

  ⑤ **已经吐过字之后，还换不换家？**
     换家 = 重放 = 客户端看到两段拼接的答案，而且不可撤销。
     真机上要制造这个场景需要"首帧之后连接被截断"——
     这个脚本用 Content-Length 声明大于实际写入的方式**故意制造**
     premature close，逼网关在"首帧已出"之后遇到错误信号。

为什么需要**四段运行**（四次启动服务，约 1.5 分钟）：
  · A1 / A2 —— 打真实上游（DeepSeek）。因为只有真实上游能回答
    "转移到第二家之后，拿到的是不是真答案"。
    A1 把不可达的候选取放在**链尾**，A2 放在**链首**：两者对比才能
    证明"顺序即优先级"（A2 必然更慢，且 failover 指标只在 A2 增长）。
  · B 段 —— 两个本地桩（一家可控失败、一家永远正常并计数）。
    因为"上游收到了几次请求"这种**精确计数**只有能计数的上游能给，
    而且要在一次运行里连续制造多种故障（失败 / 400 / 流式失败 / 首帧后截断）。
  · C 段 —— 同样的两个桩，但把熔断阈值调小。因为熔断状态活在进程里、
    无法重置，要观察"打开之后"就必须用一个专门为它配的环境，
    否则前面几段的失败会顺手把熔断器推开来，后面的断言全部失真。

桩/候选配置通过 SPRING_APPLICATION_JSON 注入 —— **不修改任何配置文件**。
真实 provider 的那一份是从 config/application-local.yml **只读**解析出来的
（那里有密钥；注入列表必须整体给出，不能只改其中一项）。

用法::

    docker compose up -d redis postgres
    python scripts/check-failover.py

可覆盖的环境变量：GATEWAY_DIR / F2_LOG / GATEWAY_PORT / F2_MODEL /
F2_MVN_CMD / F2_MAVEN_REPO / JAVA_HOME / F2_SKIP_REAL（跳过 A 段）/
F2_REAL_BASE_URL / F2_REAL_API_KEY。
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
LOG = Path(os.environ.get("F2_LOG") or (ROOT / "out" / "check-failover.log"))
PORT = int(os.environ.get("GATEWAY_PORT") or 8080)
STUB_A_PORT = int(os.environ.get("F2_STUB_A_PORT") or 18766)
STUB_B_PORT = int(os.environ.get("F2_STUB_B_PORT") or 18767)
MODEL = os.environ.get("F2_MODEL") or "deepseek-flash"
SKIP_REAL = os.environ.get("F2_SKIP_REAL") == "1"

RUN_TAG = time.strftime("%H%M%S")
FEATURE = "f2-check"

# 应用的韧性参数（与 src/main/resources/application.yml 一致）。
# 硬编码是刻意的：这些数字若与应用不一致，断言会失败 —— 那正是我们想知道的。
MAX_ATTEMPTS = 3          # gateway.resilience.retry.max-attempts
MAX_PROVIDERS = 2         # gateway.resilience.failover.max-providers
MAX_TOTAL_ATTEMPTS = 4    # gateway.resilience.failover.max-total-attempts

# C 段专用：把熔断阈值调小，让"打开"这件事在 1~2 个请求内必然发生。
# 之所以要调小而不是打满 8 次：熔断状态无法重置，打满意味着 C 段的每个
# 断言都要等前面消耗掉一整轮窗口，既慢又难读。
C_MIN_CALLS = 3
C_WINDOW = 3

STUB_ANSWER = "桩B的答案"
STUB_STREAM_TAIL = "流式后半段"
TRUNCATED_FIRST_FRAME = "首帧已送达"

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
    if os.environ.get("F2_MVN_CMD"):
        return os.environ["F2_MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo():
    if os.environ.get("F2_MAVEN_REPO"):
        return os.environ["F2_MAVEN_REPO"]
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


def error_type_of(body):
    try:
        return (body.get("error") or {}).get("type")
    except Exception:
        return None


def metric_value(name, tags=None):
    """
    从 /actuator/prometheus 取一个计数器的值。缺失返回 0.0。

    带标签的计数器在 Prometheus 文本格式里长这样：
        llmgw_upstream_retry_total{provider="provider-a"} 2.0
    所以不能只按名字匹配 —— 那是 A 批之前留下的坑（当时所有计数器都无标签）。
    F2 的 failover 用的是两个标签（from / to），必须同时匹配两个。
    """
    code, raw = api_get("/actuator/prometheus")
    if code != 200:
        return None
    if not tags:
        m = re.search(r"^%s\s+([0-9.eE+-]+)$" % re.escape(name), raw, re.M)
        return float(m.group(1)) if m else 0.0
    pattern = "^%s\\{[^}]*%s[^}]*\\}\\s+([0-9.eE+-]+)$" % (
        re.escape(name),
        ".*".join(r'%s="%s"' % (re.escape(k), re.escape(v)) for k, v in tags.items()),
    )
    m = re.search(pattern, raw, re.M)
    return float(m.group(1)) if m else 0.0


def failover_count(frm, to):
    return metric_value("llmgw_upstream_failover_total", {"from": frm, "to": to})


def circuit_all():
    code, data = api_json("/admin/circuit")
    if code != 200 or not isinstance(data, dict):
        return None
    return data


def circuit_state(provider):
    data = circuit_all()
    if not data:
        return None
    return ((data.get("providers") or {}).get(provider) or {}).get("state")


# ============================================================ 本地桩上游
STUB_A = "a"   # 可控失败的那家（链首）
STUB_B = "b"   # 永远正常并计数的那家（备选）


class StubState:
    """一个桩的可控状态。每个桩一份实例 —— 两家要能独立制造故障。"""

    def __init__(self, label):
        self.label = label
        self.lock = threading.Lock()
        self.chat_calls = 0
        self.stream_calls = 0
        self.always_fail = False          # 非流式：一直失败
        self.stream_always_fail = False   # 流式：一直失败（与上面分开，互相不干扰）
        self.fail_next = 0                # 接下来 N 次失败
        self.status = 500                 # 失败时用的状态码（测 4xx 不转移时改它）
        self.truncate_next = 0            # 接下来 N 次：吐一帧后按 Content-Length 截断

    def reset(self):
        with self.lock:
            self.chat_calls = 0
            self.stream_calls = 0
            self.always_fail = False
            self.stream_always_fail = False
            self.fail_next = 0
            self.status = 500
            self.truncate_next = 0


def make_handler(state):
    class StubHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        stub = state

        def log_message(self, fmt, *args):      # 静音
            pass

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

        def _truncated_sse(self):
            """
            吐一帧完整 SSE、然后**故意**提前关闭连接（制造 premature close）。

            这是真机上唯一能在"首帧已经交给客户端之后"制造错误信号的办法：
            SSE 一旦开始推帧，正常的结束形式就只剩"连接断开"，
            而那是正常结束、不产生 onError —— 于是 F2 的"首帧后不换家"
            这道闸门在真机上永远走不到。
            声明一个比实际写入更大的 Content-Length，Netty 就会在连接关闭时
            判定响应不完整并向上抛错，而已经解码出来的那一帧照常下发。
            """
            frame = json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                "choices": [{"index": 0,
                                             "delta": {"content": TRUNCATED_FIRST_FRAME},
                                             "finish_reason": None}]})
            body = ("data: %s\n\n" % frame).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(body) + 4096))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            time.sleep(0.4)          # 留出时间让客户端先把这一帧处理掉
            self.close_connection = True

        def do_GET(self):
            if self.path.startswith("/__stats"):
                with self.stub.lock:
                    self._json(200, {
                        "label": self.stub.label,
                        "chat_calls": self.stub.chat_calls,
                        "stream_calls": self.stub.stream_calls,
                        "always_fail": self.stub.always_fail,
                        "stream_always_fail": self.stub.stream_always_fail,
                        "fail_next": self.stub.fail_next,
                        "status": self.stub.status,
                        "truncate_next": self.stub.truncate_next,
                    })
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
                self.stub.reset()
                self._json(200, {"ok": True})
                return
            if self.path.startswith("/__fail"):
                qs = {}
                if "?" in self.path:
                    for kv in self.path.split("?", 1)[1].split("&"):
                        if "=" in kv:
                            k, v = kv.split("=", 1)
                            qs[k] = v
                with self.stub.lock:
                    if qs.get("always") == "1":
                        self.stub.always_fail = True
                    if qs.get("stream_always") == "1":
                        self.stub.stream_always_fail = True
                    if "n" in qs:
                        self.stub.fail_next = int(qs["n"])
                    if "status" in qs:
                        self.stub.status = int(qs["status"])
                self._json(200, {"ok": True})
                return
            if self.path.startswith("/__truncate"):
                qs = {}
                if "?" in self.path:
                    for kv in self.path.split("?", 1)[1].split("&"):
                        if "=" in kv:
                            k, v = kv.split("=", 1)
                            qs[k] = v
                with self.stub.lock:
                    self.stub.truncate_next = int(qs.get("n", "1"))
                self._json(200, {"ok": True})
                return
            if self.path.startswith("/__recover"):
                with self.stub.lock:
                    self.stub.always_fail = False
                    self.stub.stream_always_fail = False
                    self.stub.fail_next = 0
                    self.stub.truncate_next = 0
                    self.stub.status = 500
                self._json(200, {"ok": True})
                return

            if self.path.startswith("/v1/embeddings"):
                self._json(404, {"error": {"message": "no embedding endpoint",
                                           "type": "not_found"}})
                return

            if self.path.startswith("/v1/chat/completions"):
                is_stream = bool(payload.get("stream"))
                with self.stub.lock:
                    if is_stream:
                        self.stub.stream_calls += 1
                        self.stub.chat_calls += 1
                        if self.stub.truncate_next > 0:
                            mode = "truncate"
                            self.stub.truncate_next -= 1
                        else:
                            mode = "fail" if (self.stub.always_fail
                                              or self.stub.stream_always_fail) else "ok"
                    else:
                        self.stub.chat_calls += 1
                        mode = "fail" if (self.stub.always_fail
                                          or self.stub.fail_next > 0) else "ok"
                        if self.stub.fail_next > 0:
                            self.stub.fail_next -= 1
                    fail_status = self.stub.status

                if mode == "fail":
                    self._json(fail_status, {"error": {"message": "stub upstream failure",
                                                       "type": "server_error"}})
                    return
                if mode == "truncate":
                    self._truncated_sse()
                    return

                usage = {"prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 40}
                if is_stream:
                    self._sse([
                        json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                    "choices": [{"index": 0,
                                                 "delta": {"role": "assistant"},
                                                 "finish_reason": None}]}),
                        json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                    "choices": [{"index": 0,
                                                 "delta": {"content": STUB_ANSWER},
                                                 "finish_reason": None}]}),
                        json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                    "choices": [{"index": 0,
                                                 "delta": {"content": STUB_STREAM_TAIL},
                                                 "finish_reason": None}]}),
                        json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                    "choices": [{"index": 0, "delta": {},
                                                 "finish_reason": "stop"}]}),
                        json.dumps({"id": "stub", "object": "chat.completion.chunk",
                                    "choices": [], "usage": usage}),
                    ])
                    return

                self._json(200, {
                    "id": "stub-%s" % self.stub.label, "object": "chat.completion",
                    "choices": [{"index": 0,
                                 "message": {"role": "assistant", "content": STUB_ANSWER},
                                 "finish_reason": "stop"}],
                    "usage": usage,
                })
                return

            self._json(404, {"error": {"message": "not found", "type": "not_found"}})

    return StubHandler


STATE_A = StubState(STUB_A)
STATE_B = StubState(STUB_B)


class QuietStubServer(ThreadingHTTPServer):
    """
    关掉时会被 atexit 之外的路径打断连接，socketserver 默认会把
    ConnectionResetError 的完整栈打到 stdout —— 那是**预期内**的：
    脚本结束时应用被杀，桩上还挂着 keep-alive 连接。上百行栈会淹没
    真正的断言输出，所以这里只静音这一种，其余照打。
    """

    def handle_error(self, request, client_address):
        import sys
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


def stub_stat(port, key):
    try:
        r = NO_PROXY_OPENER.open("http://127.0.0.1:%d/__stats" % port, timeout=5)
        return json.loads(r.read().decode("utf-8")).get(key)
    except Exception:
        return None


def stub_cmd(port, path):
    try:
        req = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path),
                                     data=b"", method="POST")
        NO_PROXY_OPENER.open(req, timeout=5).read()
        return True
    except Exception as e:
        log("  [!] 调用桩控制面失败 %s%s: %r" % (port, path, e))
        return False


# ============================================================ 真实 provider 配置（只读）
def read_real_provider():
    """
    从 config/application-local.yml **只读**解析出真实 provider 的配置。

    为什么必须解析而不是硬编码：SPRING_APPLICATION_JSON 对 List 不做逐项合并，
    要"在真实 provider 之外再加一家"就必须把整份列表连同密钥一起给出。
    密钥在那个 gitignored 的文件里，脚本只读它、不改它。

    解析失败返回 None —— 上层据此跳过 A 段，而不是带着空密钥去跑出一堆假结论。
    """
    path = ROOT / "config" / "application-local.yml"
    if not path.exists():
        log("  [!] 找不到 %s，A 段将跳过" % path)
        return None
    text = path.read_text(encoding="utf-8", errors="replace")

    env_url = os.environ.get("F2_REAL_BASE_URL")
    env_key = os.environ.get("F2_REAL_API_KEY")

    name = env_url or None
    base_url = env_url
    api_key = env_key
    embedding = ""
    models = []

    if "providers:" not in text:
        return None
    tail = text.split("providers:", 1)[1]
    # 取 providers 列表的第一项：下一个 "- name:" 之前的内容
    chunks = re.split(r"^\s*-\s+name:", tail, flags=re.M)
    block = chunks[1] if len(chunks) > 1 else ""

    def grab(key):
        m = re.search(r"^\s*%s:\s*(.+?)\s*$" % re.escape(key), block, re.M)
        if not m:
            return None
        return m.group(1).strip().strip('"').strip("'")

    if name is None:
        m = re.match(r"\s*([^\s#]+)", block)
        name = (m.group(1) if m else "deepseek").strip().strip('"').strip("'")
    base_url = base_url or grab("base-url")
    api_key = api_key or grab("api-key")
    embedding = grab("embedding-model") or ""
    raw_models = grab("models") or ""
    if raw_models.startswith("["):
        models = [s.strip().strip('"').strip("'")
                  for s in raw_models[1:raw_models.find("]")].split(",") if s.strip()]
    if not (base_url and api_key):
        log("  [!] 私有配置里读不到 base-url / api-key，A 段将跳过")
        return None
    if not models:
        models = ["deepseek-flash", "deepseek-v4-pro"]
    return {
        "name": name,
        "base-url": base_url,
        "api-key": api_key,
        "embedding-model": embedding,
        "models": models,
    }


def dead_provider(name="provider-dead"):
    """链上那个"不可达部署"：端口 1 一定连不上（WebClientRequestException）。"""
    return {
        "name": name,
        "base-url": "http://127.0.0.1:1/v1",
        "api-key": "dead-key",
        "embedding-model": "",
        "models": [MODEL, "deepseek-v4-pro"],
    }


def stub_provider(name, port):
    return {
        "name": name,
        "base-url": "http://127.0.0.1:%d/v1" % port,
        "api-key": "stub-key",
        "embedding-model": "",
        "models": [MODEL, "deepseek-v4-pro"],
    }


# ============================================================ 服务生命周期
def start_app(providers=None, resilience=None, label=""):
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"
    hijacked = {k: v for k, v in env.items()
                if k in ("SERVER__PORT", "SERVER_PORT", "SPRING_APPLICATION_JSON")}
    for k in hijacked:
        env.pop(k, None)

    payload = {}
    if providers:
        payload["gateway"] = {"providers": providers}
    if resilience:
        payload.setdefault("gateway", {})["resilience"] = resilience
    if payload:
        # ★ 用 SPRING_APPLICATION_JSON 覆盖，而不是改 config/application-local.yml：
        #   1) 它的优先级高于配置文件，且**整体替换** providers 列表（正是我们要的）；
        #   2) 不落地任何文件，因此不存在"跑完忘了恢复"的风险 ——
        #      脚本中途被打断也不会把开发者的真实 base-url 留在桩地址上。
        env["SPRING_APPLICATION_JSON"] = json.dumps(payload, ensure_ascii=False)

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
    if providers:
        log("  [注入] 候选链顺序：%s" % [p["name"] for p in providers])
    if resilience:
        log("  [注入] 韧性参数覆盖：%s" % json.dumps(resilience, ensure_ascii=False))

    proc = subprocess.Popen(cmd, cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            bufsize=1, universal_newlines=False)
    tail = collections.deque(maxlen=500)

    def reader():
        for raw in iter(proc.stdout.readline, b""):
            tail.append(raw.decode("utf-8", errors="replace").rstrip())

    threading.Thread(target=reader, daemon=True).start()
    return proc, tail


def wait_ready(tail, timeout=300):
    t0 = time.time()
    log("  等待 127.0.0.1:%d 开始监听（最长 %ds）..." % (PORT, timeout))
    while time.time() < t0 + timeout:
        if any("APPLIED FAILED TO START" in l or "APPLICATION FAILED TO START" in l
               or "BUILD FAILURE" in l for l in tail):
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
    log("A 段 · 真实上游：候选顺序是不是真的等于优先级")
    log("=" * 72)

    real = read_real_provider()
    if not real:
        log("  [跳过] 无法读出真实 provider 配置")
        return

    # ---------- A1：坏候选在**链尾** ----------
    # 期望：与只配一家时完全一样 —— 正常 200、延迟正常、failover 计数为 0。
    # 这条守的是"候选链不会被遍历/探测"：如果实现是"先把候选都探一遍再挑"，
    # 那么链尾多一家坏的也会让每个请求都变慢。
    log()
    log("  A1 · 候选链 = [%s(真实), %s(不可达)] —— 坏候选在链尾" % (real["name"], "provider-dead"))
    before_fo = failover_count(real["name"], "provider-dead")
    status, elapsed, body, raw = ask("f2-a1-%s" % RUN_TAG, "一句话说明什么是故障转移", 256)
    content = answer_of(body)
    after_fo = failover_count(real["name"], "provider-dead")

    log("    HTTP=%s 耗时=%.0fms 正文=%d 字" % (status, elapsed * 1000, len(content)))
    log("    failover(%s→provider-dead) 增量=%s" % (real["name"], after_fo - before_fo))
    a1_ok = status == 200 and len(content) > 0 and (after_fo - before_fo) == 0
    if a1_ok:
        log("    [PASS] 链尾的坏候选没有任何代价：请求由链首正常服务，没有发生转移")
    else:
        log("    [FAIL] 期望 HTTP=200、正文非空、failover 增量为 0")
    results.append(("A1 链尾坏候选零代价", a1_ok,
                    "HTTP=%s 正文%d字 转移%d次" % (status, len(content), int(after_fo - before_fo))))
    baseline_ms = elapsed * 1000

    circuit = circuit_all()
    providers_in_panel = list(((circuit or {}).get("providers") or {}).keys())
    log("    /admin/circuit 里的 provider：%s" % providers_in_panel)
    results.append(("A1' 面板列出两家候选", len(providers_in_panel) == 2,
                    "providers=%s" % providers_in_panel))

    return {"real": real, "baseline_ms": baseline_ms, "providers": providers_in_panel}


def run_real_failover(results, ctx):
    """A2：坏候选在链首 —— 请求必须被透明转移到真实 provider。"""
    log()
    log("=" * 72)
    log("A2 · 真实上游：链首不可达 → 透明转移到第二家")
    log("=" * 72)
    real = ctx["real"]
    dead = dead_provider("provider-dead")

    before_retry = metric_value("llmgw_upstream_retry_total", {"provider": "provider-dead"})
    before_fo = failover_count("provider-dead", real["name"])

    status, elapsed, body, raw = ask("f2-a2-%s" % RUN_TAG, "一句话说明什么是熔断", 256)
    content = answer_of(body)
    after_retry = metric_value("llmgw_upstream_retry_total", {"provider": "provider-dead"})
    after_fo = failover_count("provider-dead", real["name"])
    retry_delta = after_retry - before_retry
    fo_delta = after_fo - before_fo

    log("    HTTP=%s 耗时=%.0fms 正文=%d 字" % (status, elapsed * 1000, len(content)))
    log("    retry(provider-dead) 增量=%d（期望 %d）" % (retry_delta, MAX_ATTEMPTS - 1))
    log("    failover(provider-dead→%s) 增量=%d（期望 1）" % (real["name"], fo_delta))
    log("    延迟对照：A1 基线 %.0fms → A2 %.0fms" % (ctx["baseline_ms"], elapsed * 1000))

    ok = (status == 200 and len(content) > 0 and fo_delta == 1
          and retry_delta == MAX_ATTEMPTS - 1)
    if ok:
        log("    [PASS] 链首连不上时被透明转移：客户端拿到的是第二家真实模型的答案")
        log("           注意代价 —— 转移之前要先把链首的重试预算烧完（%d 次），"
            % MAX_ATTEMPTS)
        log("           所以「链首坏」比「链尾坏」贵得多，这正是顺序即优先级的直接后果：")
        log("           好处是不会在坏链路上反复试（熔断随后接管），代价是刚开始那段会慢。")
    else:
        log("    [FAIL] 期望 HTTP=200、正文非空、转移 1 次、retry 增量 %d" % (MAX_ATTEMPTS - 1))
    results.append(("A2 链首不可达透明转移", ok,
                    "HTTP=%s 正文%d字 转移%d次 retry+%d 耗时%.0fms"
                    % (status, len(content), int(fo_delta), int(retry_delta), elapsed * 1000)))
    return ok


# ============================================================ B 段：桩（转移与预算）
def run_stub_failover(results):
    log()
    log("=" * 72)
    log("B 段 · 两个本地桩：转移了几次、总预算是多少")
    log("=" * 72)

    A = "provider-a"
    B = "provider-b"
    stub_cmd(STUB_A_PORT, "/__reset")
    stub_cmd(STUB_B_PORT, "/__reset")

    # --- B0：预检 —— 覆盖到底生效了没有 ---
    # 这段存在的理由：base-url 的覆盖方式如果没生效，后面的断言会全部
    # "看起来通过"（请求打到了真实上游，一切都正常），而实际上一次都没经过桩。
    # 必须先用桩自己的计数把这件事钉死。
    status, elapsed, body, raw = ask("f2-b0-%s" % RUN_TAG, "桩预检-%s" % RUN_TAG, 256)
    calls_a = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b = stub_stat(STUB_B_PORT, "chat_calls")
    log("  预检请求 HTTP=%s，桩A收到 %s 次 / 桩B收到 %s 次" % (status, calls_a, calls_b))
    if not calls_a:
        log("  [FAIL] 桩A一次请求都没收到 —— base-url 覆盖没有生效，B 段全部无效")
        results.append(("B0 桩覆盖生效", False, "桩A收到 0 次"))
        return
    log("  [PASS] 覆盖生效；且正常路径下桩B收到 %s 次 —— 没有发生转移" % calls_b)
    results.append(("B0 桩覆盖生效 + 正常路径不转移", calls_b == 0,
                    "桩A=%s 桩B=%s" % (calls_a, calls_b)))

    # --- B1：转移 = 真的换了另一家（上游计数为证） ---
    stub_cmd(STUB_A_PORT, "/__reset")
    stub_cmd(STUB_B_PORT, "/__reset")
    stub_cmd(STUB_A_PORT, "/__fail?always=1")
    fo0 = failover_count(A, B)
    rt0 = metric_value("llmgw_upstream_retry_total", {"provider": A})
    status, elapsed, body, raw = ask("f2-b1-%s" % RUN_TAG, "转移计数-%s" % RUN_TAG, 256)
    calls_a = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b = stub_stat(STUB_B_PORT, "chat_calls")
    fo1 = failover_count(A, B)
    rt1 = metric_value("llmgw_upstream_retry_total", {"provider": A})
    content = answer_of(body)

    log()
    log("  桩A一直 500；桩B正常")
    log("  HTTP=%s 正文=%r" % (status, content))
    log("  桩A收到 %s 次（期望 %d）/ 桩B收到 %s 次（期望 1）"
        % (calls_a, MAX_ATTEMPTS, calls_b))
    log("  failover 增量=%d（期望 1）/ retry(A) 增量=%d（期望 %d）"
        % (fo1 - fo0, rt1 - rt0, MAX_ATTEMPTS - 1))
    b1_ok = (status == 200 and content == STUB_ANSWER and calls_a == MAX_ATTEMPTS
             and calls_b == 1 and (fo1 - fo0) == 1 and (rt1 - rt0) == MAX_ATTEMPTS - 1)
    if b1_ok:
        log("  [PASS] 真的换了另一家：桩B给出了答案，且桩A被试满了重试预算才放弃")
        log("         桩A收到 3 次而不是 1 次，正是「重试在内、转移在外」这个嵌套顺序的证据：")
        log("         先在同一家把该试的试完，再换地方。反过来会表现为桩A只收到 1 次。")
    else:
        log("  [FAIL] 期望 HTTP=200 正文=%r、桩A=%d 桩B=1、转移 1 次"
            % (STUB_ANSWER, MAX_ATTEMPTS))
    results.append(("B1 转移真的换了另一家", b1_ok,
                    "桩A=%s 桩B=%s 正文=%r" % (calls_a, calls_b, content[:12])))

    # --- B2：总预算（把 maxAttempts × 候选家数 这个乘法关住） ---
    stub_cmd(STUB_A_PORT, "/__reset")
    stub_cmd(STUB_B_PORT, "/__reset")
    stub_cmd(STUB_A_PORT, "/__fail?always=1")
    stub_cmd(STUB_B_PORT, "/__fail?always=1")
    status, elapsed, body, raw = ask("f2-b2-%s" % RUN_TAG, "预算上限-%s" % RUN_TAG, 256)
    calls_a = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b = stub_stat(STUB_B_PORT, "chat_calls")
    total = calls_a + calls_b
    etype = error_type_of(body)

    log()
    log("  桩A、桩B 都一直 500")
    log("  HTTP=%s type=%s 耗时=%.0fms 上游总调用=%d（上限 %d）"
        % (status, etype, elapsed * 1000, total, MAX_TOTAL_ATTEMPTS))
    log("  分布：桩A=%s 桩B=%s" % (calls_a, calls_b))
    b2_ok = status == 502 and etype == "upstream_error" and total <= MAX_TOTAL_ATTEMPTS
    if b2_ok:
        log("  [PASS] 跨 provider 的总调用次数没有超过上限；失败如实报 502 upstream_error")
        log("         没有上限的话这里是 %d 次（maxAttempts × 候选家数）——"
            % (MAX_ATTEMPTS * MAX_PROVIDERS))
        log("         故障期的账单就是照着这个乘法放大的。")
    else:
        log("  [FAIL] 期望 HTTP=502 type=upstream_error 且总调用 ≤ %d" % MAX_TOTAL_ATTEMPTS)
    results.append(("B2 跨 provider 总预算生效", b2_ok,
                    "HTTP=%s 总调用=%s/%d" % (status, total, MAX_TOTAL_ATTEMPTS)))

    # --- B3：4xx 不转移 ---
    stub_cmd(STUB_A_PORT, "/__reset")
    stub_cmd(STUB_B_PORT, "/__reset")
    stub_cmd(STUB_A_PORT, "/__fail?always=1&status=400")
    status, elapsed, body, raw = ask("f2-b3-%s" % RUN_TAG, "400 不转移-%s" % RUN_TAG, 256)
    calls_a = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b = stub_stat(STUB_B_PORT, "chat_calls")
    log()
    log("  桩A一直返回 400（确定性错误）")
    log("  HTTP=%s type=%s 桩A=%s 桩B=%s" % (status, error_type_of(body), calls_a, calls_b))
    b3_ok = calls_b == 0 and calls_a == 1
    if b3_ok:
        log("  [PASS] 400 既不重试也不转移 —— 换一家会拿到一模一样的错，转移只是多买一次失败")
    else:
        log("  [FAIL] 期望桩A=1、桩B=0")
    results.append(("B3 确定性 4xx 不转移", b3_ok, "桩A=%s 桩B=%s" % (calls_a, calls_b)))

    # --- B4：流式首片前失败 → 转移，且正文不重复 ---
    stub_cmd(STUB_A_PORT, "/__reset")
    stub_cmd(STUB_B_PORT, "/__reset")
    stub_cmd(STUB_A_PORT, "/__fail?stream_always=1")
    status, chunks, done, raw = ask_stream("f2-b4-%s" % RUN_TAG, "流式转移-%s" % RUN_TAG, 256)
    text = delta_text(chunks)
    calls_a = stub_stat(STUB_A_PORT, "stream_calls")
    calls_b = stub_stat(STUB_B_PORT, "stream_calls")
    expect = STUB_ANSWER + STUB_STREAM_TAIL

    log()
    log("  桩A的流一直失败；桩B正常")
    log("  HTTP=%s [DONE]=%s 正文=%r" % (status, done, text))
    log("  桩A stream=%s（期望 %d）/ 桩B stream=%s（期望 1）" % (calls_a, MAX_ATTEMPTS, calls_b))
    b4_ok = (status == 200 and text == expect and calls_b == 1 and calls_a == MAX_ATTEMPTS)
    if b4_ok:
        log("  [PASS] 首片之前可以转移，且客户端的正文恰好是备选那一段（没有拼接重复）")
    else:
        log("  [FAIL] 期望正文=%r 且桩A=%d 桩B=1" % (expect, MAX_ATTEMPTS))
    results.append(("B4 流式首片前转移无重复", b4_ok,
                    "正文=%r 桩A=%s 桩B=%s" % (text[:16], calls_a, calls_b)))

    # --- B5：首帧之后连接被截断 → 绝不换家 ---
    stub_cmd(STUB_A_PORT, "/__reset")
    stub_cmd(STUB_B_PORT, "/__reset")
    stub_cmd(STUB_A_PORT, "/__truncate?n=1")
    status, chunks, done, raw = ask_stream("f2-b5-%s" % RUN_TAG, "首帧后截断-%s" % RUN_TAG, 256)
    text = delta_text(chunks)
    calls_b = stub_stat(STUB_B_PORT, "stream_calls")

    log()
    log("  桩A：吐一帧后按 Content-Length 截断连接（制造「首帧已送达 + 报错」）")
    log("  HTTP=%s [DONE]=%s 客户端收到正文=%r" % (status, done, text))
    log("  桩B stream=%s（期望 0 —— 闸门必须拦住它）" % calls_b)
    b5_ok = calls_b == 0
    if b5_ok:
        log("  [PASS] 首帧已经交给客户端之后，没有换到另一家重放")
        log("         如果换家，正文会变成 %r —— 用户看到「一个首帧 + 一整段答案」，"
            % (TRUNCATED_FIRST_FRAME + expect))
        log("         而且服务端日志里只会有一条漂亮的 failing over。")
    else:
        log("  [FAIL] 桩B被调用了 %s 次 —— 闸门没拦住，客户端会看到两段拼接的内容" % calls_b)
    results.append(("B5 首帧后不转移（闸门）", b5_ok,
                    "首帧=%s 桩B=%s" % (TRUNCATED_FIRST_FRAME in text, calls_b)))

    # --- B6：failover 指标带 from/to 两个标签 ---
    fo_ab = failover_count(A, B)
    fo_ba = failover_count(B, A)
    log()
    log("  failover{from=a,to=b}=%s  failover{from=b,to=a}=%s" % (fo_ab, fo_ba))
    b6_ok = fo_ab > 0 and fo_ba == 0
    if b6_ok:
        log("  [PASS] 指标能区分方向 —— 「A→B」与「B→A」是两种完全相反的事故")
        log("         （前者是「备选在扛」，后者是「两家在互相踢皮球」）")
    else:
        log("  [FAIL] 期望 from=a,to=b 有值且反向为 0")
    results.append(("B6 failover 指标带方向标签", b6_ok,
                    "a→b=%s b→a=%s" % (fo_ab, fo_ba)))


# ============================================================ C 段：熔断后的跳过
def run_stub_circuit(results):
    log()
    log("=" * 72)
    log("C 段 · 两个本地桩：熔断打开之后会不会继续被敲")
    log("=" * 72)
    log("  环境：minimum-number-of-calls=%d / sliding-window=%d（调小以便确定性打开）"
        % (C_MIN_CALLS, C_WINDOW))

    A = "provider-a"
    B = "provider-b"
    stub_cmd(STUB_A_PORT, "/__reset")
    stub_cmd(STUB_B_PORT, "/__reset")

    # --- C1：第一家被熔断 ---
    stub_cmd(STUB_A_PORT, "/__fail?always=1")
    status1, _e, body1, _r = ask("f2-c1-%s" % RUN_TAG, "熔断前-%s" % RUN_TAG, 256)
    calls_a1 = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b1 = stub_stat(STUB_B_PORT, "chat_calls")
    state_a = circuit_state(A)
    log()
    log("  第 1 个请求：桩A一直失败 / 桩B正常")
    log("  HTTP=%s 正文=%r" % (status1, answer_of(body1)))
    log("  桩A=%s 桩B=%s  → A 的熔断状态=%s" % (calls_a1, calls_b1, state_a))
    c1_ok = status1 == 200 and answer_of(body1) == STUB_ANSWER and state_a == "OPEN"
    if c1_ok:
        log("  [PASS] A 的窗口被打满 → 熔断打开；这一轮请求仍由 B 兜住（200）")
    else:
        log("  [FAIL] 期望 HTTP=200、正文=%r、A 状态 OPEN" % STUB_ANSWER)
    results.append(("C1 失败把 A 的熔断器打开", c1_ok,
                    "HTTP=%s state=%s" % (status1, state_a)))

    # --- C2：熔断打开后 A 被**跳过**，而不是"先试一下再换家" ---
    status2, _e, body2, _r = ask("f2-c2-%s" % RUN_TAG, "熔断后-%s" % RUN_TAG, 256)
    calls_a2 = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b2 = stub_stat(STUB_B_PORT, "chat_calls")
    log()
    log("  第 2 个请求（A 已 OPEN）")
    log("  HTTP=%s 正文=%r" % (status2, answer_of(body2)))
    log("  桩A=%s → %s（增量为 0 才算跳过）  桩B=%s → %s"
        % (calls_a1, calls_a2, calls_b1, calls_b2))
    c2_ok = status2 == 200 and calls_a2 == calls_a1 and calls_b2 == calls_b1 + 1
    if c2_ok:
        log("  [PASS] A 一次都没有被敲 —— 跳过，而不是「先试一次被拒再换家」")
        log("         后者的代价是：备选只剩 1 次重试机会（预算被那次白扣的调用吃掉了）")
        log("         于是主力长时间不可用时，每个请求都被静默削掉重试能力。")
    else:
        log("  [FAIL] 期望桩A计数不变、桩B +1")
    results.append(("C2 OPEN 的候选被跳过", c2_ok,
                    "桩A %s→%s 桩B %s→%s" % (calls_a1, calls_a2, calls_b1, calls_b2)))

    # --- C3：两家都熔断 → 一次调用都不发，503 ---
    stub_cmd(STUB_B_PORT, "/__fail?always=1")
    status3, _e, body3, _r = ask("f2-c3-%s" % RUN_TAG, "两家都坏-%s" % RUN_TAG, 256)
    calls_a3 = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b3 = stub_stat(STUB_B_PORT, "chat_calls")
    state_b = circuit_state(B)
    log()
    log("  第 3 个请求：桩B也改成一直失败")
    log("  HTTP=%s type=%s" % (status3, error_type_of(body3)))
    log("  桩A=%s（不变）/ 桩B=%s  B 的熔断状态=%s" % (calls_a3, calls_b3, state_b))

    status4, elapsed4, body4, _r = ask("f2-c4-%s" % RUN_TAG, "全熔断-%s" % RUN_TAG, 256)
    calls_a4 = stub_stat(STUB_A_PORT, "chat_calls")
    calls_b4 = stub_stat(STUB_B_PORT, "chat_calls")
    log("  第 4 个请求（期望两家都 OPEN）")
    log("  HTTP=%s type=%s 耗时=%.0fms" % (status4, error_type_of(body4), elapsed4 * 1000))
    log("  桩A %s→%s / 桩B %s→%s" % (calls_a3, calls_a4, calls_b3, calls_b4))
    c3_ok = (status4 == 503 and error_type_of(body4) == "circuit_open"
             and calls_a4 == calls_a3 and calls_b4 == calls_b3)
    if c3_ok:
        log("  [PASS] 两家都不健康 → 503 circuit_open 快速失败，上游一次调用都没发生")
        log("         503 而不是 502 是刻意的：502 是「我替你问了、上游答得不对」，")
        log("         503 是「我现在不打算替你去问」。客户端该退避，不该猛打。")
        log("         附带观察到一件事：熔断器会在**一次请求中途**翻到 OPEN ——")
        log("         第 3 个请求里 B 的重试进行到一半就够阈值了，第 3 次尝试被当场拒绝，")
        log("         于是那一次请求的失败类型是 circuit_open 而不是 upstream_error。")
        log("         这是对的：网关真的没有发那次请求。但它意味着「同一个请求可能")
        log("         前半段是上游错误、后半段是熔断」，排查时别把两者当成互斥的。")
    else:
        log("  [FAIL] 期望 HTTP=503 type=circuit_open 且两桩计数不变")
    results.append(("C3 全候选不可用快速失败", c3_ok,
                    "HTTP=%s type=%s 桩A %s→%s 桩B %s→%s"
                    % (status4, error_type_of(body4), calls_a3, calls_a4, calls_b3, calls_b4)))


# ============================================================ 依赖与收尾
def deps_missing():
    """
    检查 Redis / Postgres 容器是否在跑。返回缺失的容器名列表；无法判定返回 None。

    为什么值得一个前置检查：本脚本的请求链路依赖 Redis（配额）与 Postgres（账本）。
    它们挂掉时 QuotaService 会 fail-open（放行 + 记日志），于是每条断言都会红 ——
    而看起来像代码 bug。
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


def phase(results, name, providers, resilience=None, body=None):
    """启动服务 → 跑一段断言 → 关掉服务。返回该段用到的上下文。"""
    log()
    log("-" * 72)
    log("启动服务 · %s" % name)
    log("-" * 72)
    proc, tail = start_app(providers=providers, resilience=resilience)
    try:
        if not wait_ready(tail):
            log("  [!] 服务未就绪，本段中止")
            dump_tail(tail)
            return None
        return body(proc, tail)
    finally:
        stop_app(proc)
        log("  服务已关闭，端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))


def main():
    ok = False
    results = []
    stub_a = stub_b = None
    try:
        log("=" * 72)
        log("llm-gateway F2 验证（跨 provider 故障转移）  批次=%s" % RUN_TAG)
        log("=" * 72)
        log("  模型=%s  maxAttempts=%d  maxProviders=%d  maxTotalAttempts=%d"
            % (MODEL, MAX_ATTEMPTS, MAX_PROVIDERS, MAX_TOTAL_ATTEMPTS))

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
        for p in (STUB_A_PORT, STUB_B_PORT):
            if tcp_ok(p):
                log("  [!] 桩端口 %d 已被占用，先释放（或设 F2_STUB_A_PORT/F2_STUB_B_PORT）" % p)
                return

        # ---------- A 段 ----------
        ctx = None
        real = read_real_provider() if not SKIP_REAL else None
        if SKIP_REAL or not real:
            log()
            log("A 段 · 已跳过（%s）" % ("F2_SKIP_REAL" if SKIP_REAL else "读不到真实 provider 配置"))
        else:
            # A1：坏候选在链尾，请求正常打真实上游
            ctx = phase(results, "A1（真实上游 + 链尾坏候选）",
                        providers=[real, dead_provider()],
                        body=lambda proc, tail: run_real_upstream(results))
            # A2：坏候选在链首，请求必须被转移
            if ctx and ctx.get("real"):
                phase(results, "A2（真实上游 + 链首坏候选）",
                      providers=[dead_provider(), real],
                      body=lambda proc, tail: run_real_failover(results, ctx))

        # ---------- 起两个桩 ----------
        log()
        log("Step · 启动两个本地桩上游（A=%d 可控失败 / B=%d 永远正常并计数）"
            % (STUB_A_PORT, STUB_B_PORT))
        STATE_A.reset()
        STATE_B.reset()
        stub_a = QuietStubServer(("127.0.0.1", STUB_A_PORT), make_handler(STATE_A))
        stub_b = QuietStubServer(("127.0.0.1", STUB_B_PORT), make_handler(STATE_B))
        stub_a.daemon_threads = True
        stub_b.daemon_threads = True
        threading.Thread(target=stub_a.serve_forever, daemon=True).start()
        threading.Thread(target=stub_b.serve_forever, daemon=True).start()
        time.sleep(0.5)
        log("  两个桩已就绪")

        # ---------- B 段：转移与预算（熔断阈值放大，不干扰） ----------
        phase(results, "B（桩上游 · 转移与预算）",
              providers=[stub_provider("provider-a", STUB_A_PORT),
                         stub_provider("provider-b", STUB_B_PORT)],
              resilience={"circuit-breaker": {"sliding-window-size": 100,
                                             "minimum-number-of-calls": 100}},
              body=lambda proc, tail: run_stub_failover(results))

        # ---------- C 段：熔断与全候选不可用（阈值调小） ----------
        phase(results, "C（桩上游 · 熔断后跳过）",
              providers=[stub_provider("provider-a", STUB_A_PORT),
                         stub_provider("provider-b", STUB_B_PORT)],
              resilience={"circuit-breaker": {"sliding-window-size": C_WINDOW,
                                             "minimum-number-of-calls": C_MIN_CALLS}},
              body=lambda proc, tail: run_stub_circuit(results))

        # ---------- 汇总 ----------
        log()
        log("=" * 72)
        log("汇总")
        log("=" * 72)
        ok = bool(results)
        for name, good, detail in results:
            log("  [%s] %-30s %s" % ("PASS" if good else "FAIL", name, detail))
            ok = ok and good
    finally:
        for s in (stub_a, stub_b):
            if s:
                try:
                    s.shutdown()
                    s.server_close()
                except Exception:
                    pass
        log()
        log("  未修改任何配置文件（候选/桩地址经 SPRING_APPLICATION_JSON 注入）✓")
        log("  真实 provider 配置来自 config/application-local.yml 的**只读**解析 ✓")
        log()
        log("=== 结果: %s ===" % ("F2 验证全部通过 ✅" if ok else "未通过 ❌（见上方 FAIL 项）"))
        flush()


main()
