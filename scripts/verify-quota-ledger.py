# -*- coding: utf-8 -*-
"""
配额(Lua 原子性) + 成本账本落库 验证脚本。

分四段：
  A. Redis 直连：对 lua/quota.lua 做并发原子性压测（证明"超发 0"）
  B. 起网关（把配额上限用命令行压到 300 tokens）
  C. 打请求打到配额耗尽（验证应用侧配额路径 + 拿 X-Trace-Id）
  D. 查 Postgres cost_ledger：证明真落库、cost 非 0、traceId 与响应头一致

自包含：起服务 → 测 → 停服务，一个进程跑完即退。

用法::

    python scripts/verify-quota-ledger.py          # 四段全跑（需要 Redis + Postgres + 密钥）
    python scripts/verify-quota-ledger.py lua      # 只跑 A 段原子性，两秒出结果，不起服务、不花钱

可覆盖的环境变量：GATEWAY_DIR / QUOTA_LOG / GATEWAY_PORT /
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

# 默认值按「本脚本位于 <repo>/scripts/ 下」推导，clone 下来就能跑
ROOT = Path(os.environ.get("GATEWAY_DIR") or Path(__file__).resolve().parents[1])
LOG = Path(os.environ.get("QUOTA_LOG") or (ROOT / "out" / "run-quota-ledger.log"))
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
MODEL = "deepseek-flash"

QUOTA_LIMIT = 300          # 本次验证把上限压到 300 tokens
TOKENS_PER_REQ = 100       # 每题 400 字符 / 4
REQ_COUNT = 5              # 前 3 个应放行，后 2 个应被拒

QUOTA_TENANT = "quota-verify"

_lock = threading.Lock()
lines = []


def log(msg=""):
    with _lock:
        lines.append(str(msg))
    try:
        print(msg, flush=True)
    except Exception:
        pass
    # 增量落盘，方便中途观察进度
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG, "w", encoding="utf-8", errors="replace") as f:
            f.write("\n".join(lines))
    except Exception:
        pass


def mask(key):
    return "(空)" if not key else key[:7] + "*" * 6


# ============================================================ A. Redis 直连
class Redis:
    """极简 RESP 客户端。用原生 socket 而非任何三方库：
    ① 不受宿主 HTTP 代理影响；② 不引入额外依赖。"""

    def __init__(self, host="127.0.0.1", port=6379, timeout=8):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.f = self.sock.makefile("rb")

    def _encode(self, *args):
        parts = ["*%d\r\n" % len(args)]
        for a in args:
            # 注意：参数可能是 int（如 EVAL 的 numkeys），必须先 str() 再算长度，
            # 否则 len(1) 直接 TypeError（第一版就栽在这）。
            if not isinstance(a, (bytes, bytearray)):
                a = str(a)
            b = a.encode("utf-8") if isinstance(a, str) else a
            parts.append("$%d\r\n" % len(b))
            parts.append(b)
            parts.append("\r\n")
        out = b""
        for p in parts:
            out += p if isinstance(p, bytes) else p.encode("utf-8")
        return out

    def _read(self):
        line = self.f.readline()
        if not line:
            raise IOError("connection closed")
        tag, body = line[:1], line[1:].rstrip(b"\r\n")
        if tag == b"+":
            return body.decode("utf-8", "replace")
        if tag == b"-":
            raise IOError("redis error: " + body.decode("utf-8", "replace"))
        if tag == b":":
            return int(body)
        if tag == b"$":
            n = int(body)
            if n == -1:
                return None
            data = self.f.read(n + 2)[:-2]
            return data.decode("utf-8", "replace")
        if tag == b"*":
            n = int(body)
            if n == -1:
                return None
            return [self._read() for _ in range(n)]
        raise IOError("unknown reply tag %r" % tag)

    def cmd(self, *args):
        self.sock.sendall(self._encode(*args))
        return self._read()

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


def test_lua_atomicity():
    log()
    log("=" * 72)
    log("A · Lua 原子性验证（直连 Redis，绕过应用，专测脚本本身）")
    log("=" * 72)

    script = open(os.path.join(ROOT, "src/main/resources/lua/quota.lua"), encoding="utf-8").read()

    try:
        r = Redis()
        log("  Redis 连通: PING -> %s" % r.cmd("PING"))
    except Exception as e:
        log("  [FAIL] 连不上 Redis: %r" % e)
        return False

    results = {}

    def concurrent_case(name, key, tokens, limit, ttl, n):
        r.cmd("DEL", key)
        allowed, rejected, errors = [], [], []
        lock = threading.Lock()

        def worker():
            try:
                c = Redis()
                code = c.cmd("EVAL", script, 1, key,
                             str(tokens), str(limit), str(ttl))
                c.close()
                with lock:
                    (allowed if code == 0 else rejected).append(code)
            except Exception as e:
                with lock:
                    errors.append(repr(e))

        threads = [threading.Thread(target=worker) for _ in range(n)]
        t0 = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        cost_ms = (time.time() - t0) * 1000

        counter = int(r.cmd("GET", key) or 0)
        ttl_left = r.cmd("TTL", key)
        max_fit = limit // tokens

        log("  --- %s ---" % name)
        log("      参数        : %d 并发, 每次 %d tokens, 上限 %d" % (n, tokens, limit))
        log("      放行 / 拒绝  : %d / %d" % (len(allowed), len(rejected)))
        log("      计数器最终值 : %d（理论最大 %d）" % (counter, max_fit * tokens))
        log("      TTL          : %s 秒" % ttl_left)
        log("      并发耗时     : %.0f ms" % cost_ms)
        if errors:
            log("      [错误] %d 个: %s" % (len(errors), errors[:3]))

        ok = (counter <= limit) and (counter == max_fit * tokens) and not errors
        log("      结论        : %s" % ("PASS 无超发" if ok else "FAIL"))
        results[name] = ok
        return ok

    # 整除场景：60 并发 × 1 token，上限 30 -> 恰好 30 放行 / 30 拒绝
    concurrent_case("整除：60×1 token，上限 30", "verify:quota:a", 1, 30, 3600, 60)
    # 非整除场景：20 并发 × 7 tokens，上限 50 -> 最多 7 次（49），第 8 次就会超
    concurrent_case("非整除：20×7 tokens，上限 50", "verify:quota:b", 7, 50, 3600, 20)

    r.cmd("DEL", "verify:quota:a")
    r.cmd("DEL", "verify:quota:b")
    r.close()
    return all(results.values()) if results else False


# ============================================================ 通用：起服务
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


NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def start_app():
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"
    # 宿主注入了 SERVER__PORT，会被 Spring Boot 宽松绑定当成 server.port
    hijacked = {k: v for k, v in env.items()
                if k in ("SERVER__PORT", "SERVER_PORT", "SPRING_APPLICATION_JSON")}
    for k in hijacked:
        env.pop(k, None)

    args = "--server.port=%d --gateway.quota.default-limit-tokens=%d" % (PORT, QUOTA_LIMIT)
    cmd = [
        MVN, "-B", "-Duser.language=en",
        "-Dmaven.repo.local=" + REPO,
        "-f", os.path.join(ROOT, "pom.xml"),
        "spring-boot:run",
        "-Dspring-boot.run.profiles=local",
        "-Dspring-boot.run.arguments=" + args,
    ]
    log()
    log("=" * 72)
    log("B · 启动服务（配额上限覆盖为 %d tokens）" % QUOTA_LIMIT)
    log("=" * 72)
    if hijacked:
        log("  [修复] 摘除端口劫持环境变量: %s" % hijacked)
    log("  启动参数: %s" % args)
    proc = subprocess.Popen(cmd, cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
    tail = collections.deque(maxlen=400)

    def reader():
        for raw in iter(proc.stdout.readline, b""):
            tail.append(raw.decode("utf-8", errors="replace").rstrip())

    threading.Thread(target=reader, daemon=True).start()
    return proc, tail


def wait_ready(tail, timeout=360):
    t0 = time.time()
    deadline = t0 + timeout
    log("  等待 127.0.0.1:%d 监听（最长 %ds）..." % (PORT, timeout))
    last = t0
    while time.time() < deadline:
        if any("APPLICATION FAILED TO START" in l or "BUILD FAILURE" in l for l in tail):
            return "failed"
        if tcp_ok():
            log("  端口已监听，启动耗时约 %.0fs" % (time.time() - t0))
            time.sleep(2)
            return "up"
        if time.time() - last > 20:
            log("    ...编译/启动中（已 %.0fs）" % (time.time() - t0))
            last = time.time()
        time.sleep(2)
    return "timeout"


def clean(tail, n=30):
    return [l.strip() for l in tail
            if l.strip() and not l.strip().startswith(("\tat ", "Caused by:", "..."))][-n:]


# ============================================================ C. 打请求
def post(path, payload, tenant, timeout=180):
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    conn.request("POST", path, body=body, headers={
        "Content-Type": "application/json",
        "X-Api-Key": tenant,
    })
    return conn, conn.getresponse()


def test_app_quota():
    log()
    log("=" * 72)
    log("C · 应用侧配额路径（上限 %d tokens，每题 %d tokens，共 %d 题）"
        % (QUOTA_LIMIT, TOKENS_PER_REQ, REQ_COUNT))
    log("=" * 72)

    base = "这是配额验证的测试问题，请只回复收到。"
    question = base + "啊" * (400 - len(base))
    assert len(question) == 400, len(question)

    # 清掉该租户的配额键，保证从 0 起算
    try:
        r = Redis()
        period = time.strftime("%Y-%m")
        key = "q:%s:%s" % (QUOTA_TENANT, period)
        r.cmd("DEL", key)
        log("  已清空配额键 %s" % key)
    except Exception as e:
        log("  [WARN] 清理配额键失败: %r" % e)
        r = None

    accepted, rejected = [], []
    for i in range(1, REQ_COUNT + 1):
        payload = {
            "model": MODEL, "stream": False, "temperature": 0.2,
            "feature": "quota-verify",
            "messages": [{"role": "user", "content": question}],
        }
        t0 = time.time()
        try:
            conn, resp = post("/v1/chat/completions", payload, QUOTA_TENANT)
            raw = resp.read().decode("utf-8", "replace")
            ms = (time.time() - t0) * 1000
            trace = dict((h[0].lower(), h[1]) for h in resp.getheaders()).get("x-trace-id")
            conn.close()
        except Exception as e:
            log("  #%d [请求异常] %r" % (i, e))
            continue

        try:
            data = json.loads(raw)
        except Exception:
            data = {}

        if "quota exceeded" in raw:
            rejected.append(i)
            log("  #%d -> 被拒（quota exceeded）耗时 %.0f ms" % (i, ms))
        elif data.get("error"):
            log("  #%d -> 上游错误: %s" % (i, data.get("error")))
        else:
            accepted.append({"n": i, "trace": trace, "usage": data.get("usage"),
                             "model": data.get("model"), "ms": ms})
            log("  #%d -> 放行  model=%s usage=%s traceId=%s 耗时 %.0f ms"
                % (i, data.get("model"), data.get("usage"), trace, ms))

    log()
    log("  放行 %d 个 / 拒绝 %d 个（预期 %d / %d）"
        % (len(accepted), len(rejected), QUOTA_LIMIT // TOKENS_PER_REQ,
           REQ_COUNT - QUOTA_LIMIT // TOKENS_PER_REQ))

    counter = None
    if r is not None:
        try:
            counter = int(r.cmd("GET", key) or 0)
            ttl_left = r.cmd("TTL", key)
            log("  Redis 计数器 %s = %d（预期 %d，上限 %d）" % (key, counter, QUOTA_LIMIT, QUOTA_LIMIT))
            log("  TTL = %s 秒" % ttl_left)
            r.close()
        except Exception as e:
            log("  [WARN] 读取计数器失败: %r" % e)

    ok = (len(accepted) == QUOTA_LIMIT // TOKENS_PER_REQ
          and counter == QUOTA_LIMIT)
    log("  结论: %s" % ("PASS 应用路径按上限精确拦截" if ok else "FAIL"))
    return ok, accepted


# ============================================================ D. Postgres
def psql(sql, timeout=60):
    """-tA 已经就是"无表头 + 竖线分隔"，不要再加 -F"|"：
    subprocess 在 Windows 上用 cmd.exe，引号里的 | 有被解析成管道的风险。"""
    p = subprocess.run(
        'docker compose exec -T postgres psql -U postgres -d llmgw -tA -c "%s"' % sql,
        shell=True, cwd=ROOT, capture_output=True, timeout=timeout)
    out = (p.stdout or b"").decode("utf-8", "replace")
    err = (p.stderr or b"").decode("utf-8", "replace")
    return p.returncode, out, err


def test_ledger(accepted):
    log()
    log("=" * 72)
    log("D · 成本账本落库验证（Postgres cost_ledger）")
    log("=" * 72)

    # 落账是异步的，给几秒余量
    time.sleep(4)

    rc, out, err = psql(
        "SELECT count(*) FROM cost_ledger WHERE api_key='%s'" % QUOTA_TENANT)
    if rc != 0:
        log("  [FAIL] 查询失败: %s %s" % (out.strip(), err.strip()))
        return False
    rows = int(out.strip() or 0)
    log("  quota-verify 的账本行数: %d（放行请求数 %d）" % (rows, len(accepted)))

    log()
    log("  --- 最近 8 条明细 ---")
    rc, out, err = psql(
        "SELECT trace_id, model, prompt_tokens, completion_tokens, cost, latency_ms "
        "FROM cost_ledger ORDER BY id DESC LIMIT 8")
    detail = []
    for l in out.splitlines():
        if l.strip():
            log("    " + l.strip())
            detail.append(l.strip())

    log()
    log("  --- 按租户汇总 ---")
    rc, out, err = psql(
        "SELECT api_key, count(*), sum(prompt_tokens), sum(completion_tokens), "
        "round(sum(cost),6) FROM cost_ledger GROUP BY api_key ORDER BY 5 DESC")
    for l in out.splitlines():
        if l.strip():
            log("    " + l.strip())

    # cost 是否非 0（这是本轮的核心目标之一）
    rc, out, err = psql(
        "SELECT count(*) FROM cost_ledger WHERE api_key='%s' AND cost > 0" % QUOTA_TENANT)
    nonzero = int(out.strip() or 0)
    log()
    log("  cost > 0 的行数: %d（若为 0 说明单价没生效）" % nonzero)

    # traceId 与响应头交叉核对 —— 证明"全链路可观测"不是口号
    traces = [a["trace"] for a in accepted if a.get("trace")]
    matched = -1
    if traces:
        in_list = ",".join("'%s'" % t for t in traces)
        rc, out, err = psql(
            "SELECT count(*) FROM cost_ledger WHERE trace_id IN (%s)" % in_list)
        matched = int(out.strip() or 0) if rc == 0 else -1
        log("  响应头里的 traceId 共 %d 个，在账本中命中 %d 条" % (len(traces), matched))

    ok = (rows == len(accepted)) and (nonzero == len(accepted)) and (matched == len(traces))
    log()
    log("  结论: 行数一致=%s 金额非0=%s traceId贯通=%s -> %s"
        % (rows == len(accepted), nonzero == len(accepted), matched == len(traces),
           "PASS" if ok else "FAIL"))
    return ok


# ============================================================ main
def main():
    # 支持分段跑：`--only lua` 只测脚本站，`--only app` 只跑应用侧。
    # 分段的用处：改一处不必重跑整条链路（起服务一次要一分钟）。
    only = sys.argv[1] if len(sys.argv) > 1 else None
    results = {}
    proc = None
    tail = None
    try:
        if only != "app":
            results["lua"] = test_lua_atomicity()
        if only == "lua":
            log()
            log("汇总: lua=%s" % ("PASS" if results["lua"] else "FAIL"))
            return

        if tcp_ok():
            log()
            log("[!] 端口 %d 已被占用（上次残留？），中止" % PORT)
            return

        proc, tail = start_app()
        state = wait_ready(tail)
        log("  就绪判定: %s" % state)
        if state != "up":
            log()
            log("--- 应用输出（诊断用）---")
            for l in clean(tail, 40):
                log("  | " + l)
            results["app_quota"] = False
            results["ledger"] = False
        else:
            ok_q, accepted = test_app_quota()
            results["app_quota"] = ok_q
            results["ledger"] = test_ledger(accepted)

            # 顺带看一眼落库相关的指标
            log()
            log("--- Prometheus 指标（落库）---")
            try:
                r = NO_PROXY_OPENER.open("http://127.0.0.1:%d/actuator/prometheus" % PORT, timeout=5)
                body = r.read().decode("utf-8", "replace")
                for l in body.splitlines():
                    if l.startswith("llmgw_ledger") or l.startswith("llmgw_cache"):
                        log("    " + l)
            except Exception as e:
                log("    [WARN] 取指标失败: %r" % e)
    except Exception as e:
        import traceback
        log("[异常] %s" % e)
        log(traceback.format_exc())
        if tail:
            for l in list(tail)[-30:]:
                log("  | " + l)
    finally:
        if proc and proc.poll() is None:
            log()
            log("E · 关闭服务")
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
            time.sleep(3)
            log("  端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))
        if tail:
            log()
            log("--- 应用日志尾部（最后 25 行）---")
            for l in clean(tail, 25):
                log("  | " + l)

        log()
        log("=" * 72)
        log("汇总")
        log("=" * 72)
        for k, v in results.items():
            log("  %-12s: %s" % (k, "PASS" if v else "FAIL"))
        log("  总计: %s" % ("全部通过 ✅" if results and all(results.values()) else "有失败 ❌"))


if __name__ == "__main__":
    main()
