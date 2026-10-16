package com.llmgw.core.cache;

/**
 * 缓存条目 —— 两级缓存共用的返回形状。
 *
 * <p>P4 把它从 {@code SemanticCacheService} 的嵌套 record 提升为顶层类型：
 * 现在有 L1（精确）与 L2（语义）两层都返回它，若仍挂在"语义缓存"这个接口下面，
 * 精确缓存就被迫引用一个描述另一种实现的类型名。
 *
 * @param answer     命中的答案
 * @param similarity 相似度。L1（精确匹配）恒为 {@code 1.0}；L2 是余弦相似度。
 *                   保留该字段是为了让调用方统一处理，不必区分命中来自哪一层 ——
 *                   需要区分时看日志（门面会记录命中层名）。
 */
public record CacheEntry(String answer, double similarity) {
}
