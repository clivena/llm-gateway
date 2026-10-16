package com.llmgw.core.resilience;

/**
 * 上游回了 HTTP 200，但<b>响应体是空的</b>（连 JSON 都没有）。
 *
 * <h3>与 {@link UpstreamEmptyResponseException} 的区别 —— 两者都是"上游没给答案"，
 * 但坏在不同层，因此处置动作不同</h3>
 * <ul>
 *   <li>{@link UpstreamEmptyResponseException}：协议层正常，是<b>模型</b>没产出
 *       （{@code finish_reason=length} + {@code content=""}，推理型把预算花光了）。
 *       它携带完整响应，重试耗尽后要原样透传给客户端。</li>
 *   <li>{@code UpstreamEmptyBodyException}：连响应体都没有，是<b>传输/服务</b>出了问题。
 *       没有任何可以透传的东西，重试耗尽后就是 502。</li>
 * </ul>
 * 之所以分开而不是统一成"空"，是因为前者客户端应当看到
 * （"答不出来但确实被回答过，原因是被截断"），后者客户端不该看到细节
 * （网关自己没能拿到响应）。这正是 F0 那条原则的延续：
 * <b>把两种含义相反的失败合进一个类别里，就再也分不开了。</b>
 *
 * <p>继承 {@link IllegalStateException} 的原因同
 * {@link UpstreamEmptyResponseException}：泄漏到最外层时映射成 502 而不是 500 ——
 * 这是上游的问题，不是网关的 bug。
 *
 * <p>单独建类而不用 {@code IllegalStateException("empty response from upstream")}
 * 的字符串去识别，是因为重试谓词要拿它做判断：<b>靠 message 文本判断控制流，
 * 改一句文案就会静默改变重试行为</b>，而那种失效在测试里完全看不出来。
 */
public class UpstreamEmptyBodyException extends IllegalStateException {

    public UpstreamEmptyBodyException(String model) {
        super("upstream returned an empty body [model=" + model + "]");
    }
}
