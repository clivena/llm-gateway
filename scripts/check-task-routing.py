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

第 5 条尤其重要：实现里路由是**同步**解析的，若异常没有转成 Mono.error /
Flux.error，它就不会进控制器的 onErrorResume，客户端会收到 500 而不是 400 ——
单测里如果只断言异常类型，这个差别完全看不出来。

前置依赖：Redis（配额走 Redis + Lua，没有开关）。
  docker compose up -d

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
def ask(text, task=None, model="auto", stream=False, timeout=120):
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
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


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
    log()
    log("=" * 72)
    log("结果：%d/%d PASS" % (passed, len(RESULTS)))
    for name, ok, detail in RESULTS:
        log("  %s %s" % ("✓" if ok else "✗", name))
    log("=" * 72)
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
