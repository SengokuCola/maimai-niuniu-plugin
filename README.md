# 麦麦牛牛插件

麦麦牛牛插件用于在 MaiBot 中定期从匹配规则的群聊历史消息里随机抽取一条文本或图片消息，并发送回该群。

插件同时提供 `repeat_after_duplicates` 工具：当当前聊天上下文中已经出现多条完全相同的文本消息时，麦麦可以复读一次相同内容。复读工具支持普通文本，并内置长度限制、命令过滤、表情包过滤和同会话同内容冷却，避免刷屏。

插件还会在麦麦每次准备回复时，以配置概率向本次 replyer 请求注入一次性候选句提示，让模型从当前聊天历史里随机抽取的若干句中选择一句作为回复内容。

## 功能

- 按白名单或黑名单规则选择群聊。
- 定时随机抽取群聊历史消息并发送。
- 支持文本、图片内容的原样发送，不复读表情包。
- 如果目标会话最新一条消息来自麦麦自己，本次不会复读。
- 提供上下文重复文本复读工具 `repeat_after_duplicates`。
- 每次 replyer 回复前可按概率注入当前聊天历史候选句提示，默认 10% 概率抽取 10 句。

## 配置

```toml
[plugin]
enabled = true
config_version = "1.4.0"

[schedule]
interval_minutes = 15
startup_delay_seconds = 30

[chat]
mode = "blacklist"
platforms = ["qq"]
group_rules = []

[selection]
history_hours = 256.0
history_limit = 10000
min_text_length = 1

[rule]
min_message_length = 1
max_message_length = 200
cooldown_seconds = 60.0

[replyer_injection]
enabled = true
probability = 0.1
candidate_count = 10
history_limit = 200
```

`group_rules` 的格式为 `platform:id:type`，例如 `qq:123456789:group`。

`replyer_injection.probability` 为每次 replyer 回复前触发候选句提示的概率；`candidate_count` 为每次注入的候选句数量；`history_limit` 为每次最多读取多少条当前聊天历史。

## 命令和工具

- `/niuniu` 或 `/牛牛`：立即在当前群聊触发一次历史消息抽取。
- `repeat_after_duplicates`：供 LLM 在上下文出现重复文本时调用，参数 `context` 为要复读的原文。

## 许可证

MIT
