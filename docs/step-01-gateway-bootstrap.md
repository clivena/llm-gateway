# Step 01 · 网关工程底座

## 目标
让工程具备"能启动、能连中间件、配置可绑"的最小可用状态，为后续所有步骤打地基。

## 改动
- `config/WebClientConfig.java`：统一 WebClient（连接超时 3s、响应超时 60s、缓冲上限 4MB）
- `config/GatewayProperties.java`：绑定 `gateway.*`，新增 `embedding-model`、缓存条数上限、配额 TTL
- `src/main/resources/application.yml`：补全 providers / cache / quota / router 配置

## 关键代码
```java
HttpClient httpClient = HttpClient.create()
        .responseTimeout(Duration.ofSeconds(60))
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000);
WebClient.builder()
        .clientConnector(new ReactorClientHttpConnector(httpClient))
        .codecs(c -> c.defaultCodecs().maxInMemorySize(4 * 1024 * 1024));
```

## 为什么这么写
- 默认 WebClient 没有响应超时，上游卡住会拖死连接池 → 必须显式设。
- 流式响应单帧可能较大，默认 256KB 缓冲会抛 `DataBufferLimitException` → 放宽到 4MB。

## 验证方式
`mvn -q spring-boot:run` 后访问 `/actuator/health` 应为 UP。

## 坑
本机没有 JDK/Maven，这一步只做了静态检查（pom.xml XML 校验通过），未编译。

## 下一步
Step 02：把上游调用真正打通。
