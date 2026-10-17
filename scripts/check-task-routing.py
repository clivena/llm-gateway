#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务路由真机验证：按 task 选模型，选出来的模型真的被用上了吗。

这一组断言全部对着「代码看起来对、真机却不对」的形态：
  1. 显式 task=code        → 上游真的收到 stub-pro（不是配置里写了就算）
  2. 显式 task=summarize   → 上游真的收到 stub-flash
  3. 不传 task + 代码围栏  → 规则推断成 code，走 stub-pro
  4. 不传 task + 普通提问  → default-task=chat，走 stub-flash（零回归）
  5. 未知 task             → HTTP 400 + invalid_request_error，且**没有出网**
  6. 显式 model + task     → 以 model 为准（不因任务替换用户点名的模型）
  7. 流式                  → 分片里的 model 就是路由结果
  8. 指标                  → llmgw_route_task_total 带 task / source 标签
 9. 响应头                → X-Route-Task / X-Route-Source（非流式 + 流式）
10. 账本（非流式）        → cost_ledger 的 task / provider 真的按请求写进去（不是恒 NULL）
11. 账本（流式）          → 流式路径同样记 provider（两条路径取值的时机不同，要分别验）
12. 运维端点              → /admin/route 映射表+命中分布；/admin/ledger/by-provider 可按链路归因

第 5 条尤其重要：实现里路由是**同步**解析的，若异常没有转成 Mono.error /
Flux.error，它就不会进控制器的 onErrorResume，客户端会收到 500 而不是 400 ——
单测里如果只断言异常类型，这个差别完全看不出来。

第 10 条同样只有真机能验：列加了、SQL 改了，但绑参漏了或绑成空串，
单测里看到的只是"调用了一个方法"，只有数据库里的那一行能证明它真的写对了。
provider 一起验的理由：账本里"哪家答的"是从真正执行的那一步取的，
写错成常量、或与 model 混成一个字段，只有在 model 相同 provider 不同的场景下才暴露。

前置依赖：Redis（配额走 Redis + Lua，没有开关）+ Postgres（G4 要读账本）。
  docker compose up -d
  已有库需要先应用迁移：db/migrations/001-cost-ledger-task.sql
                          db/migrations/002-cost-ledger-provider.sql

用法：
  python scripts/check-task-routing.py

不修改任何配置文件：provider / candidates / 任务表全部经 SPRING_APPLICATION_JSON 注入。
"""
import collections
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("GW_PORT", "8080"))
STUB_PORT = int(os.environ.get("F3_STUB_PORT", "18791"))
BASE = "http://127.0.0.1:%d" % PORT

FLASH = "stub-flash"
PRO = "stub-pro"

RESULTS = []
# 断言条数下限。改脚本时新增/删除断言要同步改这个数 —— 它的作用不是记账，
# 而是让"某一段被跳过"这件事从静默通过变成红色失败。
EXPECTED_CASES = 15
NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def log(msg=""):
    print(str(msg), flush=True)


def _pick_mvn():
    if os.environ.get("F3_MVN_CMD"):
        return os.environ["F3_MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo():
    if os.environ.get("F3_MAVEN_REPO"):
        return os.environ["F3_MAVEN_REPO"]
    local = Path.home() / ".workbuddy/binaries/maven/repo"
    return str(local) if local.exists() else str(Path.home() / ".m2" / "repository")


MVN = _pick_mvn()
REPO = _pick_repo()
JAVA_HOME = os.environ.get("JAVA_HOME", "")


# ============================================================ 桩：记录真的收到了哪个模型
SEEN = []
LOCK = threading.Lock()


def seen():
    with LOCK:
        return list(SEEN)


def reset_seen():
    with LOCK:
        SEEN.clear()


class StubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except Exception:
            body = {}
        model = body.get("model")
        with LOCK:
            SEEN.append(model)

        if body.get("stream"):
            self._sse(model)
        else:
            self._json({
                "id": "chatcmpl-stub",
                "object": "chat.completion",
                "model": model,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "桩的答案"},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            })

    def _json(self, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _sse(self, model):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()

        delta = {"id": "c", "object": "chat.completion.chunk", "model": model,
                 "choices": [{"index": 0, "delta": {"content": "桩"}, "finish_reason": None}]}
        finish = {"id": "c", "object": "chat.completion.chunk", "model": model,
                  "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
        for payload in (delta, finish):
            self.wfile.write(("data: " + json.dumps(payload, ensure_ascii=False) + "\n\n")
                             .encode("utf-8"))
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


class QuietStub(ThreadingHTTPServer):
    """应用被杀时桩上还挂着 keep-alive 连接，默认会把上百行栈打到 stdout 淹没断言。"""

    daemon_threads = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


def start_stub():
    server = QuietStub(("127.0.0.1", STUB_PORT), StubHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.5)
    log("  桩已启动：127.0.0.1:%d（同时提供 %s / %s）" % (STUB_PORT, FLASH, PRO))
    return server


# ============================================================ 配置注入与生命周期
def task_config():
    return {
        "gateway": {
            "providers": [{
                "name": "stub",
                "base-url": "http://127.0.0.1:%d/v1" % STUB_PORT,
                "api-key": "stub-key",
                "embedding-model": "",
                "models": [FLASH, PRO],
            }],
            "router": {
                "policy": "COST_FIRST",
                "candidates": [
                    {"model": FLASH, "tier": "CHEAP",
                     "cost-per-1k": 0.001, "quality-score": 0.72},
                    {"model": PRO, "tier": "STRONG",
                     "cost-per-1k": 0.008, "quality-score": 0.90},
                ],
                "task-routing": {
                    "enabled": True,
                    "default-task": "chat",
                    "classifier": "rule",
                    "tasks": {
                        "code": {"model": PRO},
                        "extract-json": {"model": FLASH},
                        "summarize": {"model": FLASH},
                        "chat": {"model": FLASH},
                    },
                },
            },
            # 关掉缓存与韧性：本脚本只关心"选了哪个模型"，重试会改变上游收到的次数。
            "cache": {"enabled": False},
            "resilience": {
                "retry": {"enabled": False},
                "circuit-breaker": {"enabled": False},
            },
        }
    }


def start_app():
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"
    for key in ("SERVER__PORT", "SERVER_PORT", "SPRING_APPLICATION_JSON"):
        env.pop(key, None)
    env["SPRING_APPLICATION_JSON"] = json.dumps(task_config(), ensure_ascii=False)

    cmd = [
        MVN, "-B", "-Duser.language=en",
        "-Dmaven.repo.local=" + REPO,
        "-f", os.path.join(ROOT, "pom.xml"),
        "spring-boot:run",
        "-Dspring-boot.run.profiles=local",
        "-Dspring-boot.run.arguments=--server.port=%d" % PORT,
    ]
    log("  启动网关（profile=local，任务表经 SPRING_APPLICATION_JSON 注入）...")
    proc = subprocess.Popen(cmd, cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            bufsize=1)
    tail = collections.deque(maxlen=400)

    def reader():
        for raw in iter(proc.stdout.readline, b""):
            tail.append(raw.decode("utf-8", errors="replace").rstrip())

    threading.Thread(target=reader, daemon=True).start()
    return proc, tail


def port_open(port, timeout=1.5):
    sock = socket.socket()
    sock.settimeout(timeout)
    try:
        return sock.connect_ex(("127.0.0.1", port)) == 0
    finally:
        sock.close()


def wait_ready(tail, timeout=300):
    began = time.time()
    while time.time() < began + timeout:
        if any("APPLICATION FAILED TO START" in line or "BUILD FAILURE" in line for line in tail):
            return False
        if port_open(PORT):
            log("  端口已监听（启动耗时约 %.0fs）" % (time.time() - began))
            return True
        time.sleep(2)
    return False


def stop_app(proc):
    if proc and proc.poll() is None:
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                       capture_output=True)
        time.sleep(3)


# ============================================================ 请求
def _request(text, task=None, model="auto", stream=False, timeout=120):
    """发一次请求，把状态码、正文和**响应头**都带回来。

    响应头必须单独取：X-Route-* 只存在于头里（body 保持纯 OpenAI 形状），
    而 "头有没有设" 和 "body 里的 model 对不对" 是两件独立的事 ——
    前者断言的是诊断信息，后者断言的是实际选中的模型。
    """
    body = {"model": model, "messages": [{"role": "user", "content": text}]}
    if task is not None:
        body["task"] = task
    if stream:
        body["stream"] = True

    req = urllib.request.Request(
        BASE + "/v1/chat/completions",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-Api-Key": "task-check"},
    )
    try:
        with NO_PROXY_OPENER.open(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace"), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace"), dict(e.headers)


def ask(text, task=None, model="auto", stream=False, timeout=120):
    status, body, _ = _request(text, task=task, model=model, stream=stream, timeout=timeout)
    return status, body


def ask_headers(text, task=None, model="auto", stream=False, timeout=120):
    return _request(text, task=task, model=model, stream=stream, timeout=timeout)


def get_json(path, timeout=20):
    with NO_PROXY_OPENER.open(BASE + path, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def hget(headers, name):
    """大小写不敏感地取响应头。

    直接把 headers 转成 dict 会丢掉 email.message 的大小写不敏感语义，
    而"头名大小写"不该成为一条断言的失败原因 —— 那是 HTTP 规范保证的事。
    """
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return None


def wait_ledger_row(trace_id, timeout=10.0):
    """等这一条 trace 对应的账本行出现。

    落账是异步的（订阅后写库、流式还要等 onComplete），所以必须轮询。
    用 traceId 匹配而不是"取最新一行"：后者在并发下会验到别人的账，
    而且分不清"这次写对了"和"上一次碰巧写对了"。
    """
    if not trace_id:
        return None
    deadline = time.time() + timeout
    while time.time() < deadline:
        for r in get_json("/admin/ledger/recent?limit=50"):
            if r.get("trace_id") == trace_id:
                return r
        time.sleep(0.4)
    return None


def ledger_view(row):
    keys = ("task", "provider", "model")
    return {k: row.get(k) for k in keys} if row else None


def record(name, ok, detail):
    RESULTS.append((name, ok, detail))
    log("  %s %s —— %s" % ("PASS" if ok else "FAIL", name, detail))


# ============================================================ 断言
def run_cases():
    log()
    log("=" * 72)
    log("A 段 · 显式任务：配置里写的模型，上游真的收到了吗")
    log("=" * 72)

    reset_seen()
    code, _ = ask("帮我重构一下这段逻辑", task="code")
    got = seen()
    record("A1 显式 task=code 走强模型",
           code == 200 and got == [PRO],
           "HTTP=%s 上游收到=%s" % (code, got))

    reset_seen()
    code, _ = ask("总结一下这段话", task="summarize")
    got = seen()
    record("A2 显式 task=summarize 走便宜模型",
           code == 200 and got == [FLASH],
           "HTTP=%s 上游收到=%s" % (code, got))

    log()
    log("=" * 72)
    log("B 段 · 推断与兜底：不传 task 时行为是否仍然可预期")
    log("=" * 72)

    reset_seen()
    code, _ = ask("看看这段：\n```java\nint a = 1;\n```")
    got = seen()
    record("B1 代码围栏被推断成 code",
           code == 200 and got == [PRO],
           "HTTP=%s 上游收到=%s" % (code, got))

    reset_seen()
    code, body = ask("今天天气怎么样")
    got = seen()
    served = ""
    try:
        served = json.loads(body).get("model", "")
    except Exception:
        pass
    record("B2 普通提问落 default-task（零回归）",
           code == 200 and got == [FLASH] and served == FLASH,
           "HTTP=%s 上游收到=%s 响应 model=%s" % (code, got, served))

    log()
    log("=" * 72)
    log("C 段 · 错误路径：写错任务名必须是 400，而且不出网")
    log("=" * 72)

    reset_seen()
    code, body = ask("随便问问", task="cdoe")
    got = seen()
    etype = ""
    try:
        etype = json.loads(body).get("error", {}).get("type", "")
    except Exception:
        pass
    record("C1 未知任务 → 400 invalid_request_error",
           code == 400 and etype == "invalid_request_error" and got == [],
           "HTTP=%s type=%s 上游收到=%s" % (code, etype, got))

    log()
    log("=" * 72)
    log("D 段 · 显式模型优先：任务不能替换用户点名的模型")
    log("=" * 72)

    reset_seen()
    code, body = ask("总结一下这段话", task="summarize", model=PRO)
    got = seen()
    record("D1 model=pro + task=summarize 仍走 pro",
           code == 200 and got == [PRO],
           "HTTP=%s 上游收到=%s" % (code, got))

    log()
    log("=" * 72)
    log("E 段 · 流式：分片里的 model 就是路由结果")
    log("=" * 72)

    reset_seen()
    code, body = ask("```java\nint a = 1;\n```", task="code", stream=True)
    got = seen()
    models = set()
    for line in body.splitlines():
        if line.startswith("data: ") and "[DONE]" not in line:
            try:
                models.add(json.loads(line[6:]).get("model"))
            except Exception:
                pass
    record("E1 流式同样走路由结果",
           code == 200 and got == [PRO] and models == {PRO},
           "HTTP=%s 上游收到=%s 分片 model=%s" % (code, got, sorted(models)))

    log()
    log("=" * 72)
    log("F 段 · 指标：路由决策带 task / source 标签")
    log("=" * 72)

    try:
        with NO_PROXY_OPENER.open(BASE + "/actuator/prometheus", timeout=20) as resp:
            metrics = resp.read().decode("utf-8", errors="replace")
        has_code = 'llmgw_route_task_total' in metrics and 'task="code"' in metrics
        has_explicit = 'source="explicit"' in metrics
        record("F1 llmgw.route.task 指标可用",
               has_code and has_explicit,
               "task=code:%s source=explicit:%s" % (has_code, has_explicit))
    except Exception as e:
        record("F1 llmgw.route.task 指标可用", False, "读取失败：%r" % (e,))

    log()
    log("=" * 72)
    log("G 段 · 诊断能力：判定结果与成本归因能不能被外部看到")
    log("=" * 72)

    # G1 / G2：响应头。这两条的失败形态是"请求全对、只是头没设"，
    # 单测能覆盖接线，但覆盖不了"Spring 有没有把它写出去"。
    status, _, headers = ask_headers("总结一下这段话", task="summarize")
    task_h = hget(headers, "X-Route-Task")
    source_h = hget(headers, "X-Route-Source")
    record("G1 非流式响应头带 X-Route-Task / X-Route-Source",
           status == 200 and task_h == "summarize" and source_h == "explicit",
           "HTTP=%s task=%s source=%s" % (status, task_h, source_h))

    status, _, headers = ask_headers("看看这段：\n```java\nint a = 1;\n```")
    task_h = hget(headers, "X-Route-Task")
    source_h = hget(headers, "X-Route-Source")
    record("G2 推断来源可区分（inferred 而不是 explicit）",
           status == 200 and task_h == "code" and source_h == "inferred",
           "HTTP=%s task=%s source=%s" % (status, task_h, source_h))

    status, _, headers = ask_headers("```java\nint a = 1;\n```", task="code", stream=True)
    task_h = hget(headers, "X-Route-Task")
    record("G3 流式响应头同样带路由结果",
           status == 200 and task_h == "code",
           "HTTP=%s task=%s" % (status, task_h))

    # G4 / G6：账本。用**这次响应的 traceId** 去库里找那一行 ——
    # 这比"看最新一行"强得多：它同时证明了响应与账本属于同一次请求，
    # 以及 task 是跟着这次判定走的，而不是碰巧被上一条请求写进去的。
    # （第一次实现用 id 递增做判据，而 /admin/ledger/recent 当时不返回 id，
    #  于是判据恒假、断言恒失败 —— 一个"看起来在验、其实什么都没验"的教训。）
    try:
        status, _, headers = ask_headers("帮我重构一下这段逻辑", task="code")
        trace_id = hget(headers, "X-Trace-Id")
        row = wait_ledger_row(trace_id)

        # provider 与 task / model 一起断言：它是 P7 的落点。model 相同、provider 不同的
        # 情况下，只验 model 会看不出账本到底记没记"是哪家答的"。
        record("G4 账本按请求写入 task / provider 列（非流式）",
               status == 200 and row is not None
               and row.get("task") == "code" and row.get("model") == PRO
               and row.get("provider") == "stub",
               "trace=%s 行=%s" % (trace_id, ledger_view(row)))
    except Exception as e:
        record("G4 账本按请求写入 task / provider 列（非流式）", False,
               "读取失败：%r（/admin/ledger/recent 依赖 Postgres）" % (e,))

    # G6：流式也要记 provider。它不是 G4 的重复 —— 两条路径取 provider 的时机不同：
    # 非流式在拿到响应体的那一刻就知道是谁答的；流式根本拿不到"响应对象"，
    # 只能在开始拉流时记下 adapter 名。只验非流式，等于把一半实现留在未验证状态。
    try:
        status, _, headers = ask_headers("```java\nint a = 1;\n```", task="code", stream=True)
        trace_id = hget(headers, "X-Trace-Id")
        row = wait_ledger_row(trace_id)

        record("G6 流式账本同样写入 provider / task",
               status == 200 and row is not None
               and row.get("task") == "code" and row.get("provider") == "stub"
               and row.get("model") == PRO,
               "trace=%s 行=%s" % (trace_id, ledger_view(row)))
    except Exception as e:
        record("G6 流式账本同样写入 provider / task", False, "读取失败：%r" % (e,))

    # G5：/admin/route。它必须同时给出"配置生效成什么样"和"实际命中分布"，
    # 只回显配置是不够的 —— 排障要回答的是"现在到底在往哪边倒"。
    try:
        route = get_json("/admin/route")
        tasks = route.get("tasks") or {}
        hits = route.get("hits") or {}
        record("G5 /admin/route 给出映射表与命中分布",
               route.get("enabled") is True
               and (tasks.get("code") or {}).get("model") == PRO
               and (tasks.get("chat") or {}).get("model") == FLASH
               and (hits.get("code") or {}).get("explicit", 0) >= 1
               and route.get("hitsScope") == "since-process-start",
               "enabled=%s code→%s chat→%s hits.code=%s"
               % (route.get("enabled"),
                  (tasks.get("code") or {}).get("model"),
                  (tasks.get("chat") or {}).get("model"),
                  hits.get("code")))
    except Exception as e:
        record("G5 /admin/route 给出映射表与命中分布", False, "读取失败：%r" % (e,))

    # G7：按 provider 归因的聚合端点。它存在的意义只有一个 ——
    # F2 之后同一个 model 可能在多家部署，"备用那家接管了多少流量"这个问题
    # 用 model 维度回答不了。所以这里验的是"有 stub 这一行且 calls > 0"，
    # 顺带把 task / model 维度之外的第二条归因轴打通。
    try:
        rows = get_json("/admin/ledger/by-provider")
        stub_row = next((r for r in rows if r.get("provider") == "stub"), None)
        record("G7 /admin/ledger/by-provider 能按链路归因",
               stub_row is not None and int(stub_row.get("calls") or 0) > 0,
               "providers=%s stub=%s"
               % ([r.get("provider") for r in rows], stub_row))
    except Exception as e:
        record("G7 /admin/ledger/by-provider 能按链路归因", False, "读取失败：%r" % (e,))


def main():
    log("=" * 72)
    log("任务路由真机验证（按 task 选模型）")
    log("=" * 72)

    if not port_open(6379, timeout=1):
        log()
        log("[中止] 127.0.0.1:6379 未监听 —— 配额走 Redis + Lua，没有开关可以绕开。")
        log("       先执行：docker compose up -d")
        return 2
    if port_open(PORT):
        log()
        log("[中止] %d 已被占用，先关掉正在运行的网关。" % PORT)
        return 2

    stub = start_stub()
    proc, tail = start_app()
    try:
        if not wait_ready(tail):
            log()
            log("[失败] 网关没有起来，最后 40 行日志：")
            for line in list(tail)[-40:]:
                log("  | " + line)
            return 2

        run_cases()
    finally:
        stop_app(proc)
        stub.shutdown()

    passed = sum(1 for _, ok, _ in RESULTS if ok)

    # 条数下限：判据 passed == len(RESULTS) 只能保证"跑了的都通过"，
    # 保证不了"该跑的都跑了"。上一轮 check-failover 就栽在这里 ——
    # 分段启动失败被静默 return None，汇总照样打印"全部通过"。
    if len(RESULTS) < EXPECTED_CASES:
        log()
        log("[失败] 只跑了 %d 条断言，少于预期 %d 条 —— 有段落被跳过。"
            % (len(RESULTS), EXPECTED_CASES))
        return 1

    log()
    log("=" * 72)
    log("结果：%d/%d PASS" % (passed, len(RESULTS)))
    for name, ok, detail in RESULTS:
        log("  %s %s" % ("✓" if ok else "✗", name))
    log("=" * 72)
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
