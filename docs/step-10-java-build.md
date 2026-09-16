# Step 10 · 装 Maven 并让 Java 侧编译通过

## 目标
把"代码写了但没跑过"这个缺口补上：编译 Java 侧 + 跑通单测。

## 做法：独立目录安装（不动系统环境）
- 下载 Apache Maven 3.9.11 到独立目录（`~/.workbuddy/binaries/maven/`），不动系统环境
- 依赖仓库也隔离：`-Dmaven.repo.local=<...>/maven/repo`
- 复用已装的 JDK 24，运行前设 `JAVA_HOME`

```bash
mvn -B -Dstyle.color=never -Duser.language=en -Dmaven.repo.local=<isolated-repo> test
```

## 编译错误与修法（3 类，都是真实踩坑）

| # | 报错 | 原因 | 修法 |
|---|---|---|---|
| 1 | `chatStream` 参数不匹配 | 接口签名是 `chatStream(model, request)`，调用只传了 model | 两处补上 `request` |
| 2 | `eval(...)` 找不到符号 | `ReactiveRedisConnection.eval` 的参数是 `ByteBuffer...`，不是 `byte[]...` | 改用 `ReactiveStringRedisTemplate.execute(RedisScript, keys, args)`，由模板处理序列化与脚本缓存 |
| 3 | `execute(script, keys, String...)` 不匹配 | 该重载要求 `List<?> args`，不支持可变参数 | 参数用 `List.of(...)` 包一层 |

> 附带修掉一个隐患：`onErrorResume` 三元表达式里 `Flux.error(e)` 会推断成 `Flux<Object>`，
> 显式写成 `Flux.<String>error(e)` 才类型安全。

## 结果

```
[INFO] Compiling 20 source files with javac [debug parameters release 17] to target\classes
[INFO] Running com.llmgw.core.router.RouterServiceTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

## 踩坑

- **Windows 下 Maven 输出是 GBK**：Python `subprocess(text=True)` 默认 UTF-8 会在读取线程里抛 `UnicodeDecodeError`，
  导致拿不到真正的报错。改为 `capture_output=True` + `decode("utf-8", errors="replace")` 才看清错误。
- 加 `-Duser.language=en` 可以把 javac 报错变英文，避免乱码。

## 下一步
- [ ] `mvn -q spring-boot:run` 起服务 + docker compose 起 redis/postgres，跑一次真实调用
- [ ] 用 k6/wrk 压测对比"直连上游"基线，拿到 P95 额外延迟与吞吐数字
