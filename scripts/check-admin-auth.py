#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运维接口鉴权真机验证：`/admin/**` 到底会不会被外部访问。

单测（AdminAuthFilterTest）用 mock exchange 精确构造了 remoteAddress，
但它证明不了两件事，只有真机能给：
  1. 一个真实 TCP 连接的 remoteAddress 到底是什么（代理 / 容器 / IPv6 都可能不同）；
  2. 过滤器真的挂进了 WebFlux 的链里（而不是只被 new 出来过一次）。

断言：
  A1 未配 token + 环回        → 200（本地开发零配置可用）
  A2 未配 token + 非环回      → 403 + admin_token_not_configured
  B1 配了 token + 不带头      → 401（环回也一样，不能靠"我是本机"绕过）
  B2 配了 token + 错头        → 401
  B3 配了 token + 对头        → 200
  B4 配了 token + 业务接口无头 → 不是 401/403（运维加固不能把用户挡在门外）
  B5 配了 token + 非环回带对头 → 200（远程运维是可行的，不是"配了就只许本机"）

前置依赖：Redis（/admin/status 会报 Redis 健康状态）。
  docker compose up -d
  不需要 Postgres / Kafka：本脚本只打 /admin/status 与 /v1。

用法：
  python scripts/check-admin-auth.py

不修改任何配置文件：两段配置都经 SPRING_APPLICATION_JSON 注入。
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
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("GW_PORT", "8080"))
TOKEN = "test-admin-token-9f3a"

RESULTS = []
NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def log(msg=""):
    print(str(msg), flush=True)


def record(name, ok, detail, skipped=False):
    RESULTS.append((name, ok, detail, skipped))


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


def lan_ip():
    """拿一个非环回的本机地址，用于制造"外部来源"。

    不发包：UDP connect 只做路由选择。拿不到（没网卡 / 全被禁用）返回 None，
    脚本会把相关的两条断言标成 SKIP 并在汇总里说清楚，而不是假装通过。
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        return None if ip.startswith("127.") else ip
    except OSError:
        return None
    finally:
        s.close()


LAN = lan_ip()


# ============================================================ 生命周期
def base_config(token):
    """只注入 admin.token，其余全部沿用 config/application-local.yml。

    第一版这里把 providers / candidates 也一起注入了，结果启动失败：
    SPRING_APPLICATION_JSON 是**深度合并**而不是整体替换 —— 候选表被换成
    一个假的模型，而 local 里的任务映射还指着原模型，启动校验当场拒绝。
    教训：验鉴权的脚本不该顺手改模型配置，改得越多，失败原因越难归因。
    """
    return {"gateway": {"admin": {"token": token}}}


def start_app(token):
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"
    for key in ("SERVER__PORT", "SERVER_PORT", "SPRING_APPLICATION_JSON"):
        env.pop(key, None)
    env["SPRING_APPLICATION_JSON"] = json.dumps(base_config(token), ensure_ascii=False)

    cmd = [
        MVN, "-B", "-Duser.language=en",
        "-Dmaven.repo.local=" + REPO,
        "-f", os.path.join(ROOT, "pom.xml"),
        "spring-boot:run",
        "-Dspring-boot.run.profiles=local",
        "-Dspring-boot.run.arguments=--server.port=%d" % PORT,
    ]
    proc = subprocess.Popen(cmd, cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    tail = collections.deque(maxlen=300)

    def reader():
        for raw in iter(proc.stdout.readline, b""):
            tail.append(raw.decode("utf-8", errors="replace").rstrip())

    threading.Thread(target=reader, daemon=True).start()
    return proc, tail


def port_open(port, host="127.0.0.1", timeout=1.5):
    sock = socket.socket()
    sock.settimeout(timeout)
    try:
        return sock.connect_ex((host, port)) == 0
    finally:
        sock.close()


def wait_ready(tail, timeout=300):
    began = time.time()
    while time.time() < began + timeout:
        if any("APPLICATION FAILED TO START" in l or "BUILD FAILURE" in l for l in tail):
            return False
        if port_open(PORT):
            return True
        time.sleep(2)
    return False


def stop_app(proc):
    if proc and proc.poll() is None:
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
        time.sleep(3)


# ============================================================ 请求
def get(url, headers=None, timeout=15):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with NO_PROXY_OPENER.open(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def admin(path, host="127.0.0.1", headers=None):
    return get("http://%s:%d%s" % (host, PORT, path), headers=headers)


def chat_without_body():
    """业务接口：只发一个空 JSON，期望 400，但绝不能是 401/403。"""
    req = urllib.request.Request(
        "http://127.0.0.1:%d/v1/chat/completions" % PORT,
        data=b"{}",
        headers={"Content-Type": "application/json", "X-Api-Key": "auth-check"})
    try:
        with NO_PROXY_OPENER.open(req, timeout=15) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def start_phase(title, token):
    """启动一段场景，返回 (proc, tail)；启动失败返回 (None, tail)。

    ★ 这里**不**负责停服务。第一版把 stop_app 放在 finally 里、然后 return
    "启动成功"，于是调用方拿到的是一条已经关掉的服务 —— 断言全部报
    "目标计算机积极拒绝"，看起来像"服务起不来"，实际是"服务被我关了"。
    生命周期归调用方持有，try/finally 写在调用方那一层。
    """
    log()
    log("=" * 72)
    log(title)
    log("=" * 72)
    proc, tail = start_app(token)
    if not wait_ready(tail):
        log("[失败] 网关没有起来，最后 30 行日志：")
        for line in list(tail)[-30:]:
            log("  | " + line)
        stop_app(proc)
        return None, tail
    return proc, tail


# ============================================================ 场景
def run_phase_a():
    proc, _ = start_phase("A 段 · 未配置 token：只允许本机", "")
    if proc is None:
        record("A 段启动", False, "网关未启动")
        return
    try:
        status, _ = admin("/admin/status")
        record("A1 未配 token 时本机可访问", status == 200, "HTTP=%s" % status)

        if LAN is None:
            record("A2 未配 token 时外部被拒", False,
                   "SKIP：没有非环回地址可用（无法真实构造外部来源）", skipped=True)
        else:
            status, body = admin("/admin/status", host=LAN)
            record("A2 未配 token 时外部被拒",
                   status == 403 and "admin_token_not_configured" in body,
                   "来源=%s HTTP=%s body=%s" % (LAN, status, body[:120]))
    finally:
        stop_app(proc)


def run_phase_b():
    proc, _ = start_phase("B 段 · 配置 token：必须带 X-Admin-Token", TOKEN)
    if proc is None:
        for n in ("B1", "B2", "B3", "B4", "B5"):
            record(n + " 段启动", False, "网关未启动")
        return
    try:
        status, body = admin("/admin/route")
        record("B1 配 token 后本机不带凭证也被拒",
               status == 401 and "unauthorized" in body,
               "HTTP=%s body=%s" % (status, body[:120]))

        status, _ = admin("/admin/route", headers={"X-Admin-Token": "wrong"})
        record("B2 错误的 token 被拒", status == 401, "HTTP=%s" % status)

        status, _ = admin("/admin/route", headers={"X-Admin-Token": TOKEN})
        record("B3 正确的 token 放行", status == 200, "HTTP=%s" % status)

        status = chat_without_body()
        record("B4 业务接口不受运维鉴权影响",
               status not in (401, 403), "HTTP=%s（期望 400，绝不该是 401/403）" % status)

        if LAN is None:
            record("B5 远程带对 token 可访问", False,
                   "SKIP：没有非环回地址可用", skipped=True)
        else:
            status, _ = admin("/admin/route", host=LAN, headers={"X-Admin-Token": TOKEN})
            record("B5 远程带对 token 可访问", status == 200,
                   "来源=%s HTTP=%s" % (LAN, status))
    finally:
        stop_app(proc)


def main():
    log("=" * 72)
    log("运维接口鉴权真机验证（/admin/**）")
    log("=" * 72)
    log("  非环回地址：%s" % (LAN or "（无，A2/B5 将标 SKIP）"))

    if not port_open(6379, timeout=1):
        log()
        log("[中止] 127.0.0.1:6379 未监听 —— /admin/status 需要 Redis。")
        log("       先执行：docker compose up -d")
        return 2
    if not (ROOT / "config" / "application-local.yml").exists():
        log()
        log("[中止] 缺少 config/application-local.yml（本脚本沿用该文件里的模型配置）。")
        log("       cp config/application-local.yml.example config/application-local.yml")
        return 2
    if port_open(PORT):
        log()
        log("[中止] %d 已被占用，先关掉正在运行的网关。" % PORT)
        return 2

    run_phase_a()
    run_phase_b()

    hard = [(n, ok, d) for n, ok, d, sk in RESULTS if not sk]
    soft = [(n, ok, d) for n, ok, d, sk in RESULTS if sk]
    passed = sum(1 for _, ok, _ in hard if ok)

    log()
    log("=" * 72)
    log("结果：%d/%d PASS（另有 %d 条因无局域网地址跳过）"
        % (passed, len(hard), len(soft)))
    for name, ok, detail, skipped in RESULTS:
        mark = "~" if skipped else ("✓" if ok else "✗")
        log("  %s %s —— %s" % (mark, name, detail))
    log("=" * 72)

    expected = 5 if LAN is None else 7
    if len(RESULTS) < expected:
        log("[失败] 只跑了 %d 条断言，少于预期 %d 条 —— 有段落被跳过。"
            % (len(RESULTS), expected))
        return 1
    return 0 if passed == len(hard) else 1


if __name__ == "__main__":
    sys.exit(main())
