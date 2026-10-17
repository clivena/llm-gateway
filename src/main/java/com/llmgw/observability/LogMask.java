package com.llmgw.observability;

/**
 * 日志里的凭证脱敏。
 *
 * <h3>为什么需要它</h3>
 * {@code apiKey} 在这个项目里同时是身份（配额归属）和凭证（调用上游/鉴权）。
 * 它出现在日志里的每一行，都等于把一把钥匙抄进了磁盘上的文件 ——
 * 而日志的访问面通常比配置文件宽得多：被采集、被转发、被贴进工单、被截图。
 *
 * <h3>为什么保留 4 + 4 而不是全遮</h3>
 * 排障时最常见的一句话是"这个租户的请求为什么一直失败"，需要能在日志里
 * 把同一个租户的多行串起来。全遮之后所有 key 都长成 {@code ***}，
 * 这条能力就没了，于是人们会去别处找办法（比如临时把明文打回来）。
 *
 * 保留首尾各 4 位 + 中段遮掉的折中：mask 相同的行仍然是同一个 key，
 * 但 8 位不足以还原一个正常长度的密钥。长度 ≤ 8 的直接全遮 ——
 * 那种长度的 key 保留首尾就等于保留全部。
 *
 * <p>这是**输出侧**的防护，不是存储侧的：账本表里仍然是明文
 * （对账需要稳定的租户标识），访问面由 {@code AdminAuthFilter} 控制。
 */
public final class LogMask {

    private LogMask() {
    }

    private static final String FULL = "***";
    private static final String ABSENT = "(none)";

    public static String of(String secret) {
        if (secret == null || secret.isBlank()) {
            return ABSENT;
        }
        String s = secret.trim();
        if (s.length() <= 8) {
            return FULL;
        }
        return s.substring(0, 4) + FULL + s.substring(s.length() - 4);
    }
}
