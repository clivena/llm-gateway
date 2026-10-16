# -*- coding: utf-8 -*-
"""
一次性诊断脚本（非交付物）：扫描 max_tokens，定位推理型模型"多预算才肯出正文"。

背景：gateway 的非流式路径只读 message.content，一旦上游把 max_tokens 全花在
reasoning 上，就会返回 finish_reason=length + content=""，而网关把它当成功。
本脚本量化这个阈值，供 P5（completion 估算要考虑 reasoning）与 F1/F2
（"空正文算不算失败"）决策使用。
"""
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
text = (ROOT / "config" / "application-local.yml").read_text(encoding="utf-8")
base_url = re.search(r"base-url:\s*(\S+)", text).group(1).strip().strip('"')
api_key = re.search(r'api-key:\s*"?([^"\s]+)"?', text).group(1).strip()

BUDGETS = [512, 1024, 2048, 4096]
MODELS = ["deepseek-flash", "deepseek-v4-pro"]


def probe(model, mt):
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": "什么是语义缓存"}],
        "max_tokens": mt,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/chat/completions", data=body,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + api_key},
        method="POST")
    with urllib.request.urlopen(req, timeout=300) as resp:
        return json.loads(resp.read().decode("utf-8"))


print(f"{'模型':<18}{'预算':>6}{'finish':>9}{'reason':>8}{'正文长':>8}{'总completion':>14}")
print("-" * 66)
for model in MODELS:
    for mt in BUDGETS:
        try:
            d = probe(model, mt)
        except Exception as e:
            print(f"{model:<18}{mt:>6}   请求失败: {e!r}")
            continue
        ch = (d.get("choices") or [{}])[0]
        msg = ch.get("message") or {}
        content = msg.get("content") or ""
        usage = d.get("usage") or {}
        r_tokens = (usage.get("completion_tokens_details") or {}).get("reasoning_tokens", 0)
        print(f"{model:<18}{mt:>6}{str(ch.get('finish_reason')):>9}{r_tokens:>8}"
              f"{len(content):>8}{usage.get('completion_tokens', 0):>14}")
    print()

sys.stdout.flush()
