# 麦麦牛牛插件

麦麦牛牛插件用于在 MaiBot 中定期从匹配规则的群聊历史消息里随机抽取一条消息，并发送回该群。

插件同时提供 `repeat_after_duplicates` 工具：当当前聊天上下文中已经出现多条完全相同的文本消息时，麦麦可以复读一次相同内容。复读工具支持普通文本和上下文渲染出的表情描述，并内置长度限制、命令过滤和同会话同内容冷却，避免刷屏。

## 功能

- 按白名单或黑名单规则选择群聊。
- 定时随机抽取群聊历史消息并发送。
- 支持文本、图片、表情和转发消息内容的原样发送。
- 提供上下文重复文本复读工具 `repeat_after_duplicates`。
- 支持表情描述检索并复读表情包。

## 配置

```toml
[plugin]
enabled = true
config_version = "1.3.0"

[schedule]
interval_minutes = 20
startup_delay_seconds = 30

[chat]
mode = "blacklist"
platforms = ["qq"]
group_rules = []

[selection]
history_hours = 128.0
history_limit = 1000
min_text_length = 1

[rule]
min_message_length = 1
max_message_length = 200
cooldown_seconds = 60.0
```

`group_rules` 的格式为 `platform:id:type`，例如 `qq:123456789:group`。

## 命令和工具

- `/niuniu` 或 `/牛牛`：立即在当前群聊触发一次历史消息抽取。
- `repeat_after_duplicates`：供 LLM 在上下文出现重复文本时调用，参数 `context` 为要复读的原文。

## 许可证

MIT
