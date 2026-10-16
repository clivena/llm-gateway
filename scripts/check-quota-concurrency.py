# -*- coding: utf-8 -*-
"""
llm-gateway 配额并发验证 —— Redis + Lua 原子扣减的实测护栏

它回答一个问题：**并发下配额会不会超发？**

为什么值得单独一个脚本：配额是"读-判-写"三段逻辑，只要有任何一个并发窗口，
就会出现"余额 50 却放行 60 次"。这种 bug 在单线程测试里 100% 看不出来，
只有真的同时打进去才暴露。而它又是**直接对应真金白银**的那个组件。

做法（不假设任何计量口径，全部实测）：
  1) 用**低配额**启动服务（--gateway.quota.default-limit-tokens）
  2) 用一个独立租户发 1 次请求，读 /admin/quota/{租户} **实测**每次预扣多少 token
     —— 不硬编码 length/4 的取整行为，那属于实现细节，改了这里就会误报
  3) 用另一个独立租户，N 个线程打同一时刻（threading.Barrier 对齐）发 N 次
  4) 统计 200 / 429，并读 /admin/quota 拿最终计数器
  5) 断言三件事：
       · 放行数 == LIMIT // 每次预扣     （量级比较，不是"数次数"）
       · 拒绝数 == N - 放行数
       · 计数器精确 == 放行数 × 每次预扣  （**没有超发**，这是本脚本的核心目的）
  6) 顺便核对账本新增行数 == 放行数（配额通过后不该有请求静默丢失）

用**一次性租户名**（带时间戳）而不是 tenant-a：配额 key 是 q:<租户>:<年月>，
新租户天然从 0 开始，既不必清理既有计数，也不会污染联调留下的数据。
脚本结束会打印 Redis 清理命令（不自动删：删错 key 比留个垃圾 key 严重）。

用法::

    docker compose up -d redis postgres
    python scripts/check-quota-concurrency.py

可覆盖的环境变量：GATEWAY_DIR / QUOTA_LOG / GATEWAY_PORT / QUOTA_LIMIT /
QUOTA_CONCURRENCY / QUOTA_MVN_CMD / QUOTA_MAVEN_REPO / JAVA_HOME。
"""
import collections
import http.client
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------- 路径与配置
ROOT = Path(os.environ.get("GATEWAY_DIR") or Path(__file__).resolve().parents[1])
LOG = Path(os.environ.get("QUOTA_LOG") or (ROOT / "out" / "check-quota-concurrency.log"))
PORT = int(os.environ.get("GATEWAY_PORT") or 8080)

# 配额上限刻意取小：不必打几万次请求才能触发拒绝。
# 50 与"每次预扣 7"配合时除不尽（50/7=7 余 1），
# 这样"第 8 次 49+7=56 > 50 被拒"能证明是量级比较而非数次数。
LIMIT = int(os.environ.get("QUOTA_LIMIT") or 50)
CONCURRENCY = int(os.environ.get("QUOTA_CONCURRENCY") or 20)

# 请求文本长度决定每次预扣额（实现是 max(1, len/4)）。28 字符 → 预扣 7。
# 长度是"可调参数"而不是"要断言的常量"：脚本会实测出真实值，
# 这里只需要保证它和 LIMIT 除不尽（50 // 7 = 7 余 1），才能验证"按量比较"。
FILLER = "请用一句话解释什么是语义缓存，不超过四十个字，谢谢配合哦"


def _pick_mvn() -> str:
    if os.environ.get("QUOTA_MVN_CMD"):
        return os.environ["QUOTA_MVN_CMD"]
    local = Path.home() / ".workbuddy/binaries/maven/apache-maven-3.9.11/bin/mvn.cmd"
    return str(local) if local.exists() else "mvn"


def _pick_repo() -> str:
    if os.environ.get("QUOTA_MAVEN_REPO"):
        return os.environ["QUOTA_MAVEN_REPO"]
    local = Path.home() / ".workbuddy/binaries/maven/repo"
    return str(local) if local.exists() else str(Path.home() / ".m2" / "repository")


MVN = _pick_mvn()
REPO = _pick_repo()
JAVA_HOME = os.environ.get("JAVA_HOME", "")
MODEL = "deepseek-flash"

RUN_TAG = time.strftime("%H%M%S")
PROBE_TENANT = "quota-probe-%s" % RUN_TAG
MAIN_TENANT = "quota-conc-%s" % RUN_TAG

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


# 环境里有 HTTP_PROXY 指向宿主代理，urllib 默认走代理，
# 探 127.0.0.1 会拿到代理的 502 而不是本机响应。管理端点必须绕开代理。
NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def api_get(path, timeout=10):
    """GET 一个管理端点，返回 (状态码, 解析后的 JSON 或 None)。"""
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


def post_chat(tenant, prompt, timeout=120):
    """发一次非流式请求，返回 (HTTP 状态码, 响应体 dict 或 None, 耗时秒)。"""
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    payload = {
        "model": MODEL,
        "stream": False,
        "temperature": 0.0,
        "max_tokens": 8,          # 压住放行请求的输出成本：这次只关心配额，不关心答得好不好
        "feature": "quota-concurrency",
        "messages": [{"role": "user", "content": prompt}],
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
    finally:
        try:
            conn.close()
        except Exception:
            pass
    elapsed = time.time() - t0
    try:
        data = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        data = None
    return status, data, elapsed


# ---------------------------------------------------------------- 1. 启动服务
def start_app():
    env = dict(os.environ)
    if JAVA_HOME:
        env["JAVA_HOME"] = JAVA_HOME
    env["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8"
    env["PYTHONIOENCODING"] = "utf-8"

    # 宿主注入的 SERVER__PORT 会被 Spring Boot 宽松绑定当成 server.port，
    # 让服务起在别人的端口上（实测：报 Port <注入值> was already in use）。
    # 双保险：从子进程环境摘掉 + 命令行强制覆盖。
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
        # 一定要用命令行参数而不是环境变量设配额：命令行优先级最高，
        # 且不会因为某个 export 忘了撤而让后续测试莫名偏低。
        "-Dspring-boot.run.arguments=--server.port=%d --gateway.quota.default-limit-tokens=%d"
        % (PORT, LIMIT),
    ]
    log()
    log("=" * 72)
    log("Step 1 · 启动服务（配额上限临时压到 %d tokens）" % LIMIT)
    log("=" * 72)
    if hijacked:
        log("  [修复] 检测到端口劫持环境变量，已从子进程摘除: %s" % hijacked)
    log("  [设定] --gateway.quota.default-limit-tokens=%d" % LIMIT)

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
    """就绪判定用 TCP 直连，不看 /actuator/health。

    健康端点会因 Redis/Postgres 之一未就绪而返回 503，
    拿它做就绪判断会把"服务其实已经起来了"误判成"启动失败"。
    """
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


# ---------------------------------------------------------------- 2. 实测预扣额
def measure_cost_per_request():
    """发 1 次真实请求，用管理端点读回扣减额。

    为什么不直接算 len/4：那是实现细节。哪天估算器从"字符数除以 4"换成
    "按 tokenizer 精确计数"（本项目的 P5 待办），这个脚本不该跟着一起改。
    读真实计数器 = 脚本对实现保持中立。
    """
    log()
    log("=" * 72)
    log("Step 2 · 实测每次请求的预扣额（租户 %s）" % PROBE_TENANT)
    log("=" * 72)
    log("  请求文本长度: %d 字符" % len(FILLER))
    status, data, elapsed = post_chat(PROBE_TENANT, FILLER)
    log("  HTTP 状态: %s（耗时 %.0f ms）" % (status, elapsed * 1000))
    if status != 200:
        log("  [!] 探针请求未被放行，无法测定预扣额。多半是 LIMIT 设得比单次预扣还小。")
        return None
    code, quota = api_get("/admin/quota/%s" % PROBE_TENANT)
    if code != 200 or not quota:
        log("  [!] 读配额失败: HTTP %s %s" % (code, quota))
        return None
    used = quota.get("usedTokens")
    log("  管理端点回读: usedTokens=%s（limit=%s）" % (used, quota.get("limitTokens")))
    if not used:
        log("  [!] usedTokens 为 0 或读不到 —— 探针请求没扣到配额，后续断言无意义")
        return None
    log("  => 每次请求预扣 %d tokens" % used)
    return int(used)


# ---------------------------------------------------------------- 3. 并发施压
def hammer(per_request):
    expected_pass = LIMIT // per_request
    log()
    log("=" * 72)
    log("Step 3 · 并发施压（租户 %s，%d 个线程同时发）" % (MAIN_TENANT, CONCURRENCY))
    log("=" * 72)
    log("  预期放行 = LIMIT // 每次预扣 = %d // %d = %d 次" % (LIMIT, per_request, expected_pass))
    log("  预期拒绝 = %d - %d = %d 次" % (CONCURRENCY, expected_pass, CONCURRENCY - expected_pass))
    log("  注：%d 除不尽 %d，所以最后一次是 49+%d=%d > %d 被拒 —— "
        "能通过说明扣的是量，不是次数"
        % (LIMIT, per_request, per_request, expected_pass * per_request + per_request, LIMIT))

    # Barrier 让所有线程在同一时刻冲出起跑线。若只是循环起线程，
    # 前面几个请求早已返回，并发窗口根本不存在，测了等于没测。
    barrier = threading.Barrier(CONCURRENCY)
    lock = threading.Lock()
    seen = collections.Counter()
    lat_pass, lat_deny = [], []
    errors = []

    def worker():
        try:
            barrier.wait(timeout=60)
        except Exception as e:
            with lock:
                errors.append("barrier: %r" % e)
            return
        try:
            status, body, elapsed = post_chat(MAIN_TENANT, FILLER)
        except Exception as e:
            status, body, elapsed = -1, None, 0.0
            with lock:
                errors.append(repr(e))
        with lock:
            seen[status] += 1
            (lat_pass if status == 200 else lat_deny).append(elapsed)

    threads = [threading.Thread(target=worker) for _ in range(CONCURRENCY)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.time() - t0

    passed = seen.get(200, 0)
    denied = seen.get(429, 0)
    others = {k: v for k, v in seen.items() if k not in (200, 429)}

    log("  总墙钟耗时: %.1f s" % wall)
    log("  状态码分布: 200=%d  429=%d  其他=%s" % (passed, denied, others or "无"))
    if lat_pass:
        log("  放行请求延迟: 平均 %.0f ms / 最大 %.0f ms（含真实上游调用）"
            % (sum(lat_pass) / len(lat_pass) * 1000, max(lat_pass) * 1000))
    if lat_deny:
        log("  拒绝请求延迟: 平均 %.0f ms / 最大 %.0f ms（未触及上游）"
            % (sum(lat_deny) / len(lat_deny) * 1000, max(lat_deny) * 1000))
    if errors:
        log("  [!] 异常 %d 条，样例: %s" % (len(errors), errors[:3]))
    return passed, denied, expected_pass, others


# ---------------------------------------------------------------- 4. 断言
def verify(passed, denied, expected_pass, per_request, others):
    log()
    log("=" * 72)
    log("Step 4 · 断言")
    log("=" * 72)
    checks = []

    code, quota = api_get("/admin/quota/%s" % MAIN_TENANT)
    used = quota.get("usedTokens") if quota else None
    limit_seen = quota.get("limitTokens") if quota else None
    log("  管理端点回读: usedTokens=%s / limitTokens=%s" % (used, limit_seen))

    expect_used = expected_pass * per_request
    checks.append(("放行数 == LIMIT // 每次预扣  (%d == %d)" % (passed, expected_pass),
                   passed == expected_pass))
    checks.append(("拒绝数 == 并发数 - 放行数  (%d == %d)" % (denied, CONCURRENCY - expected_pass),
                   denied == CONCURRENCY - expected_pass))
    # ← 这条才是重点：计数器必须**精确**落在放行总额上。
    #   只要有一丝并发窗口，它就会超过 limit（超发）或对不上（漏扣）。
    checks.append(("计数器精确无超发  (%s == %d)" % (used, expect_used),
                   used == expect_used))
    checks.append(("计数器未越界  (%s <= %d)" % (used, LIMIT),
                   used is not None and int(used) <= LIMIT))
    checks.append(("无意外状态码", not others))

    # 账本是异步写的，给它一点时间落库再核对
    time.sleep(3)
    lcode, summary = api_get("/admin/ledger/summary")
    ledger_rows = None
    if lcode == 200 and isinstance(summary, list):
        for row in summary:
            if row.get("apiKey") == MAIN_TENANT or row.get("api_key") == MAIN_TENANT:
                ledger_rows = row.get("calls") or row.get("count") or row.get("total")
                break
        if ledger_rows is None and summary:
            # 端点字段名可能不同，退化为打印一行原文，不硬断言
            log("  账本汇总未匹配到本租户，原文样例: %s" % json.dumps(summary[:1], ensure_ascii=False)[:200])
    if ledger_rows is not None:
        log("  账本新增行数: %s（放行数 %d）" % (ledger_rows, passed))
        checks.append(("账本行数 == 放行数  (%s == %d)" % (ledger_rows, passed),
                       int(ledger_rows) == passed))
    else:
        log("  （账本核对跳过：/admin/ledger/summary 未返回可识别字段）")

    ok = True
    for name, good in checks:
        log("  [%s] %s" % ("PASS" if good else "FAIL", name))
        ok = ok and good

    log()
    log("  Redis 清理（可选，测试租户 35 天后会自行过期）:")
    log("    docker compose exec -T redis redis-cli DEL q:%s:%s q:%s:%s"
        % (PROBE_TENANT, time.strftime("%Y-%m"), MAIN_TENANT, time.strftime("%Y-%m")))
    return ok


def main():
    proc = None
    tail = None
    ok = False
    try:
        log("=" * 72)
        log("llm-gateway 配额并发验证  租户标签=%s" % RUN_TAG)
        log("=" * 72)
        log("  配额上限   : %d tokens" % LIMIT)
        log("  并发数     : %d" % CONCURRENCY)
        log("  模型       : %s" % MODEL)

        if tcp_ok():
            log("  [!] 端口 %d 已被占用，先停掉占用进程再跑" % PORT)
            return
        log("  端口 %d 空闲，可以启动" % PORT)

        proc, tail = start_app()
        if not wait_ready(tail):
            log("  [!] 服务未就绪，中止")
            return

        per_request = measure_cost_per_request()
        if not per_request:
            return

        passed, denied, expected_pass, others = hammer(per_request)
        ok = verify(passed, denied, expected_pass, per_request, others)
    finally:
        if proc and proc.poll() is None:
            log()
            log("Step 5 · 关闭服务")
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True)
            time.sleep(3)
            log("  已发送 taskkill /F /T，端口占用: %s" % ("是" if tcp_ok() else "否（已释放）"))
        log()
        log("=== 结果: %s ===" % ("配额无超发 ✅" if ok else "未通过 ❌（见上方 FAIL 项）"))
        flush()


main()
