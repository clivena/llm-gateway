package com.llmgw.api;

import com.llmgw.config.GatewayProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OpenAI 协议里"不带业务含义"的那部分兼容端点。
 *
 * 目前只有 {@code GET /v1/models}。它本身不产生任何上游流量，
 * 但不少 OpenAI 客户端/SDK 启动时会先探一下模型列表来决定可用模型 ——
 * 缺这个端点会让它们直接报"服务不可用"，而问题其实跟模型能力无关。
 *
 * **{@code POST /v1/embeddings} 刻意没有实现**，原因不是漏了：
 * 当前上游（DeepSeek）没有 embeddings 端点（实测 HTTP 404），
 * 实现出来也只会是一个必然失败的转发。语义缓存因此在配置层关着。
 * 这条边界写在 README 的「已知边界」里，不在代码里假装支持。
 */
@RestController
@RequestMapping("/v1")
public class OpenAiCompatController {

    private final GatewayProperties properties;

    public OpenAiCompatController(GatewayProperties properties) {
        this.properties = properties;
    }

    @GetMapping("/models")
    public Map<String, Object> models() {
        // 跨 provider 去重：同一个模型名可能在多家 provider 下都配了，
        // 列表里出现两次会让客户端的下拉框出现重复项。
        Set<String> ids = new LinkedHashSet<>();
        for (GatewayProperties.Provider provider : properties.getProviders()) {
            if (provider.getModels() != null) {
                ids.addAll(provider.getModels());
            }
        }
        return OpenAiProtocol.modelsBody(new ArrayList<>(ids));
    }
}
