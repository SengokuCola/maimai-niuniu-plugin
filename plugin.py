"""麦麦牛牛插件。

定期从匹配规则的群聊中随机选择一个群，抽取一条别人发过的文本或图片消息并发送回该群。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import asyncio
import base64
import random
import re
import time

from maibot_sdk import Command, Field, HookHandler, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ErrorPolicy, HookMode, HookOrder, ToolParameterInfo, ToolParamType


NIUNIU_REPLYER_INJECTION_MARKER = "【麦麦牛牛随机复读候选】"


@dataclass(frozen=True)
class ChatRule:
    """聊天流匹配规则。"""

    platform: str
    target_id: str
    chat_type: str


@dataclass
class GroupChoice:
    """一次可抽取的群聊及其关联会话。"""

    platform: str
    group_id: str
    send_chat_id: str
    query_chat_ids: List[str]


def _extract_nested_list(payload: Any, expected_key: str) -> Optional[List[Any]]:
    """从 capability 返回值中剥离常见包装层，提取列表字段。"""

    if isinstance(payload, list):
        return payload

    current = payload
    visited: set[int] = set()
    while isinstance(current, dict):
        current_id = id(current)
        if current_id in visited:
            break
        visited.add(current_id)

        value = current.get(expected_key)
        if isinstance(value, list):
            return value

        next_value = current.get("result")
        if isinstance(next_value, dict):
            current = next_value
            continue
        next_value = current.get("data")
        if isinstance(next_value, dict):
            current = next_value
            continue
        break
    return None


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置。"""

    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="1.4.0", description="配置版本")


class ScheduleConfig(PluginConfigBase):
    """定时任务配置。"""

    __ui_label__ = "定时"
    __ui_icon__ = "timer"
    __ui_order__ = 1

    interval_minutes: int = Field(default=60, ge=1, description="抽取并发送消息的间隔，单位分钟")
    startup_delay_seconds: int = Field(default=30, ge=0, description="插件加载后首次执行前的等待秒数")


class ChatConfig(PluginConfigBase):
    """群聊选择配置。"""

    __ui_label__ = "聊天"
    __ui_icon__ = "message-circle"
    __ui_order__ = 2

    mode: str = Field(default="whitelist", description="群聊选择模式：whitelist 或 blacklist")
    platforms: List[str] = Field(default_factory=lambda: ["qq"], description="需要扫描的消息平台")
    group_rules: List[str] = Field(
        default_factory=list,
        description="群聊规则列表，格式为 platform:id:type，例如 qq:123456:group",
    )


class SelectionConfig(PluginConfigBase):
    """消息抽取配置。"""

    __ui_label__ = "抽取"
    __ui_icon__ = "shuffle"
    __ui_order__ = 3

    history_hours: float = Field(default=24.0, ge=0.1, description="抽取最近多少小时内的历史消息")
    history_limit: int = Field(default=200, ge=1, le=10000, description="每次最多读取的历史消息数量")
    min_text_length: int = Field(default=1, ge=0, description="纯文本消息的最短长度")


class RepeatRuleConfig(PluginConfigBase):
    """复读判定配置。"""

    __ui_label__ = "复读规则"
    __ui_icon__ = "message-square-repeat"
    __ui_order__ = 4

    min_message_length: int = Field(default=1, ge=1, le=1000, description="允许复读的最短消息长度")
    max_message_length: int = Field(default=200, ge=1, le=2000, description="允许复读的最长消息长度")
    cooldown_seconds: float = Field(default=60.0, ge=0.0, le=3600.0, description="同一会话同一内容的复读冷却时间")


class ReplyerInjectionConfig(PluginConfigBase):
    """回复器一次性提示注入配置。"""

    __ui_label__ = "回复注入"
    __ui_icon__ = "message-square-plus"
    __ui_order__ = 5

    enabled: bool = Field(default=True, description="是否在回复器请求中随机注入牛牛候选句提示")
    probability: float = Field(default=0.1, ge=0.0, le=1.0, description="每次回复触发候选句提示的概率")
    candidate_count: int = Field(default=10, ge=1, le=50, description="每次注入时随机提供的候选句数量")
    history_limit: int = Field(default=200, ge=1, le=10000, description="每次最多读取多少条当前聊天历史")


class NiuniuPluginConfig(PluginConfigBase):
    """麦麦牛牛插件配置模型。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    chat: ChatConfig = Field(default_factory=ChatConfig)
    selection: SelectionConfig = Field(default_factory=SelectionConfig)
    rule: RepeatRuleConfig = Field(default_factory=RepeatRuleConfig)
    replyer_injection: ReplyerInjectionConfig = Field(default_factory=ReplyerInjectionConfig)


class NiuniuPlugin(MaiBotPlugin):
    """麦麦牛牛插件。"""

    config_model = NiuniuPluginConfig

    def __init__(self) -> None:
        super().__init__()
        self._task: Optional[asyncio.Task[None]] = None
        self._last_repeat_at: Dict[Tuple[str, str], float] = {}

    async def on_load(self) -> None:
        """处理插件加载。"""

        await self._restart_task()

    async def on_unload(self) -> None:
        """处理插件卸载。"""

        await self._stop_task()

    async def on_config_update(self, scope: str, config_data: Dict[str, Any], version: str) -> None:
        """处理配置热更新。"""

        del config_data
        self.ctx.logger.info("麦麦牛牛收到配置更新: scope=%s, version=%s", scope, version)
        if scope == "self":
            await self._restart_task()

    async def _restart_task(self) -> None:
        """根据当前配置重启后台任务。"""

        await self._stop_task()
        if not self.config.plugin.enabled:
            self.ctx.logger.info("麦麦牛牛插件已禁用，跳过后台任务")
            return

        self._task = asyncio.create_task(self._run_loop(), name="maimai_niuniu_plugin.loop")
        self.ctx.logger.info("麦麦牛牛后台任务已启动")

    async def _stop_task(self) -> None:
        """停止后台任务。"""

        if self._task is None:
            return
        if not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        """定期抽取并发送历史消息。"""

        if self.config.schedule.startup_delay_seconds > 0:
            await asyncio.sleep(self.config.schedule.startup_delay_seconds)

        while True:
            try:
                await self._pick_and_send_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.ctx.logger.error("麦麦牛牛执行失败", exc_info=True)

            await asyncio.sleep(self.config.schedule.interval_minutes * 60)

    @staticmethod
    def _normalize_mode(mode: str) -> str:
        """归一化群聊选择模式。"""

        normalized_mode = mode.strip().lower()
        return normalized_mode if normalized_mode in {"whitelist", "blacklist"} else "whitelist"

    @staticmethod
    def _parse_chat_rule(rule_text: str) -> Optional[ChatRule]:
        """解析 platform:id:type 格式的群聊规则。"""

        normalized_rule = str(rule_text or "").strip()
        if not normalized_rule:
            return None

        parts = [part.strip() for part in normalized_rule.split(":")]
        if len(parts) != 3 or not all(parts):
            return None
        return ChatRule(platform=parts[0], target_id=parts[1], chat_type=parts[2].lower())

    def _parse_chat_rules(self) -> List[ChatRule]:
        """读取配置中的群聊规则。"""

        rules: List[ChatRule] = []
        for rule_text in self.config.chat.group_rules:
            rule = self._parse_chat_rule(rule_text)
            if rule is not None:
                rules.append(rule)
        return rules

    async def _get_group_streams(self) -> List[Dict[str, Any]]:
        """读取当前可见的群聊流。"""

        configured_platforms = [platform.strip() for platform in self.config.chat.platforms if platform.strip()]
        platforms = configured_platforms or ["qq"]
        streams: List[Dict[str, Any]] = []
        seen_session_ids: set[str] = set()

        for platform in platforms:
            result = await self.ctx.chat.get_group_streams(platform)
            group_streams = _extract_nested_list(result, "streams")
            if group_streams is None:
                self.ctx.logger.warning("麦麦牛牛读取群聊流返回格式异常: platform=%s result=%r", platform, result)
                continue
            for stream in group_streams:
                if not isinstance(stream, dict):
                    continue
                session_id = str(stream.get("session_id") or "").strip()
                if not session_id or session_id in seen_session_ids:
                    continue
                seen_session_ids.add(session_id)
                streams.append(stream)
        return streams

    @staticmethod
    def _build_group_choices(streams: List[Dict[str, Any]]) -> List[GroupChoice]:
        """将同一平台同一群号下的多个会话聚合起来。"""

        groups: Dict[tuple[str, str], GroupChoice] = {}
        for stream in streams:
            platform = str(stream.get("platform") or "").strip()
            group_id = str(stream.get("group_id") or "").strip()
            session_id = str(stream.get("session_id") or "").strip()
            if not platform or not group_id or not session_id:
                continue

            key = (platform, group_id)
            choice = groups.get(key)
            if choice is None:
                groups[key] = GroupChoice(
                    platform=platform,
                    group_id=group_id,
                    send_chat_id=session_id,
                    query_chat_ids=[session_id],
                )
                continue
            if session_id not in choice.query_chat_ids:
                choice.query_chat_ids.append(session_id)
        return list(groups.values())

    @staticmethod
    def _group_matches_rule(group: GroupChoice, rule: ChatRule) -> bool:
        """判断群聊是否命中某条规则。"""

        return (
            rule.chat_type in {"group", "群", "group_chat"}
            and group.platform == rule.platform
            and group.group_id == rule.target_id
        )

    async def _get_candidate_groups(self) -> List[GroupChoice]:
        """按白名单/黑名单规则获取候选群聊。"""

        rules = self._parse_chat_rules()
        groups = self._build_group_choices(await self._get_group_streams())
        mode = self._normalize_mode(self.config.chat.mode)

        if mode == "whitelist":
            return [group for group in groups if any(self._group_matches_rule(group, rule) for rule in rules)]
        return [
            group
            for group in groups
            if not any(self._group_matches_rule(group, rule) for rule in rules)
        ]

    async def _get_recent_messages_from_group(self, group: GroupChoice) -> List[Any]:
        """读取同一群聊下所有已知会话的最近消息。"""

        messages_by_id: Dict[str, Any] = {}
        anonymous_messages: List[Any] = []
        for chat_id in group.query_chat_ids:
            messages = await self.ctx.call_capability(
                "message.get_recent",
                chat_id=chat_id,
                hours=self.config.selection.history_hours,
                limit=self.config.selection.history_limit,
                limit_mode="latest",
                filter_mai=True,
            )
            recent_messages = _extract_nested_list(messages, "messages")
            if recent_messages is None:
                self.ctx.logger.warning(
                    "麦麦牛牛读取历史消息返回格式异常: chat_id=%s result=%r",
                    chat_id,
                    messages,
                )
                continue

            for message in recent_messages:
                if not isinstance(message, dict):
                    anonymous_messages.append(message)
                    continue
                message_id = str(message.get("message_id") or "").strip()
                if message_id:
                    messages_by_id[message_id] = message
                else:
                    anonymous_messages.append(message)
        return list(messages_by_id.values()) + anonymous_messages

    async def _get_recent_messages_from_chat(self, chat_id: str, limit: int, filter_mai: bool) -> List[Any]:
        """读取指定会话的最近消息。"""

        messages = await self.ctx.call_capability(
            "message.get_recent",
            chat_id=chat_id,
            hours=self.config.selection.history_hours,
            limit=limit,
            limit_mode="latest",
            filter_mai=filter_mai,
        )
        recent_messages = _extract_nested_list(messages, "messages")
        if recent_messages is None:
            self.ctx.logger.warning(
                "麦麦牛牛读取最近消息返回格式异常: chat_id=%s result=%r",
                chat_id,
                messages,
            )
            return []
        return recent_messages

    async def _get_latest_message_from_chat(self, chat_id: str) -> Optional[Dict[str, Any]]:
        """读取指定会话的最新一条消息。"""

        recent_messages = await self._get_recent_messages_from_chat(chat_id, limit=1, filter_mai=False)
        if not recent_messages:
            return None

        latest_message = recent_messages[-1]
        return latest_message if isinstance(latest_message, dict) else None

    @staticmethod
    def _message_user_id(message: Dict[str, Any]) -> str:
        """提取消息发送者 ID。"""

        user_id = str(message.get("user_id") or "").strip()
        if user_id:
            return user_id

        message_info = message.get("message_info")
        if not isinstance(message_info, dict):
            return ""
        user_info = message_info.get("user_info")
        if not isinstance(user_info, dict):
            return ""
        return str(user_info.get("user_id") or "").strip()

    @staticmethod
    def _message_platform(message: Dict[str, Any], fallback_platform: str) -> str:
        """提取消息平台。"""

        platform = str(message.get("platform") or "").strip()
        if platform:
            return platform

        message_info = message.get("message_info")
        if isinstance(message_info, dict):
            platform = str(message_info.get("platform") or "").strip()
            if platform:
                return platform
        return fallback_platform

    async def _latest_chat_message_is_self(self, chat_id: str, fallback_platform: str) -> bool:
        """判断当前会话最新一条消息是否来自麦麦自己。"""

        latest_message = await self._get_latest_message_from_chat(chat_id)
        if latest_message is None:
            return False

        user_id = self._message_user_id(latest_message)
        platform = self._message_platform(latest_message, fallback_platform)
        if not user_id or not platform:
            self.ctx.logger.warning(
                "麦麦牛牛无法判断最新消息发送者: chat_id=%s platform=%s user_id=%s message_id=%s",
                chat_id,
                platform,
                user_id,
                str(latest_message.get("message_id") or ""),
            )
            return False

        from src.chat.utils.utils import is_bot_self

        return is_bot_self(platform, user_id)

    async def _build_group_choice_from_context(self, stream_id: str, platform: str, group_id: str) -> Optional[GroupChoice]:
        """根据当前命令上下文构造当前群聊选择。"""

        normalized_stream_id = stream_id.strip()
        normalized_platform = platform.strip()
        normalized_group_id = group_id.strip()
        if not normalized_stream_id:
            return None
        if not normalized_group_id:
            return GroupChoice(
                platform=normalized_platform,
                group_id="",
                send_chat_id=normalized_stream_id,
                query_chat_ids=[normalized_stream_id],
            )

        related_session_ids: List[str] = []
        for stream in await self._get_group_streams():
            stream_platform = str(stream.get("platform") or "").strip()
            stream_group_id = str(stream.get("group_id") or "").strip()
            stream_session_id = str(stream.get("session_id") or "").strip()
            if stream_group_id != normalized_group_id:
                continue
            if normalized_platform and stream_platform != normalized_platform:
                continue
            if stream_session_id and stream_session_id not in related_session_ids:
                related_session_ids.append(stream_session_id)

        if normalized_stream_id not in related_session_ids:
            related_session_ids.append(normalized_stream_id)
        return GroupChoice(
            platform=normalized_platform or "unknown",
            group_id=normalized_group_id,
            send_chat_id=normalized_stream_id,
            query_chat_ids=related_session_ids,
        )

    async def _pick_from_group(self, group: GroupChoice) -> bool:
        """从指定群聊抽取并发送一条历史消息。"""

        if await self._latest_chat_message_is_self(group.send_chat_id, group.platform):
            self.ctx.logger.info(
                "麦麦牛牛跳过复读，最新一条消息来自麦麦自己: platform=%s group_id=%s chat_id=%s",
                group.platform,
                group.group_id,
                group.send_chat_id,
            )
            return False

        recent_messages = await self._get_recent_messages_from_group(group)
        candidates = await asyncio.to_thread(
            lambda: [message for message in recent_messages if self._is_usable_message(message)]
        )
        if not candidates:
            self.ctx.logger.info(
                "麦麦牛牛没有找到可抽取的历史消息: platform=%s group_id=%s query_chat_ids=%s",
                group.platform,
                group.group_id,
                group.query_chat_ids,
            )
            return False

        random.shuffle(candidates)
        for message in candidates:
            sent = await self._send_message(message, group.send_chat_id)
            if not sent:
                continue
            self.ctx.logger.info(
                "麦麦牛牛已发送历史消息: platform=%s group_id=%s send_chat_id=%s message_id=%s",
                group.platform,
                group.group_id,
                group.send_chat_id,
                str(message.get("message_id") or ""),
            )
            return True

        self.ctx.logger.info(
            "麦麦牛牛候选消息都无法原样发送: platform=%s group_id=%s query_chat_ids=%s candidate_count=%s",
            group.platform,
            group.group_id,
            group.query_chat_ids,
            len(candidates),
        )
        return False

    async def _pick_and_send_once(self) -> bool:
        """定时任务：随机选一个有历史消息的候选群聊抽取并发送。"""

        groups = await self._get_candidate_groups()
        if not groups:
            self.ctx.logger.warning("麦麦牛牛没有可用的源群聊，无法抽取历史消息")
            return False

        random.shuffle(groups)
        skipped_groups: List[str] = []
        for group in groups:
            if await self._pick_from_group(group):
                return True
            skipped_groups.append(f"{group.platform}:{group.group_id}")

        self.ctx.logger.info("麦麦牛牛没有找到可抽取的历史消息: tried_groups=%s", skipped_groups)
        return False

    def _is_usable_message(self, message: Any) -> bool:
        """判断历史消息是否适合被抽取。"""

        if not isinstance(message, dict):
            return False
        if message.get("is_command") or message.get("is_notify"):
            return False

        raw_message = message.get("raw_message")
        if isinstance(raw_message, list):
            raw_segments = self._raw_message_segments(message)
            if not raw_segments:
                return False
            return any(
                segment_type == "image" or len(content) >= self.config.selection.min_text_length
                for segment_type, content in raw_segments
            )

        if message.get("is_emoji"):
            return False

        plain_text = self._message_text(message)
        return bool(plain_text and len(plain_text) >= self.config.selection.min_text_length)

    @staticmethod
    def _message_text(message: Dict[str, Any]) -> str:
        """提取消息文本。"""

        for key in ("processed_plain_text", "plain_text"):
            text = str(message.get(key) or "").strip()
            if text:
                return text

        raw_message = message.get("raw_message")
        if not isinstance(raw_message, list):
            return ""
        text_parts: List[str] = []
        for segment in raw_message:
            if not isinstance(segment, dict) or segment.get("type") != "text":
                continue
            text = str(segment.get("data") or segment.get("content") or "").strip()
            if text:
                text_parts.append(text)
        return "".join(text_parts).strip()

    @staticmethod
    def _raw_text_segments(message: Dict[str, Any]) -> List[str]:
        """提取原始消息中的纯文本段。"""

        raw_message = message.get("raw_message")
        if not isinstance(raw_message, list):
            return []

        text_parts: List[str] = []
        for segment in raw_message:
            if not isinstance(segment, dict) or str(segment.get("type") or "").strip().lower() != "text":
                continue
            text = str(segment.get("data") or segment.get("content") or "").strip()
            if text:
                text_parts.append(text)
        return text_parts

    @staticmethod
    def _split_text_sentences(text: str) -> List[str]:
        """将聊天文本拆成适合提供给回复器选择的候选句。"""

        sentences: List[str] = []
        for line in text.splitlines():
            normalized_line = re.sub(r"\s+", " ", line).strip()
            if not normalized_line:
                continue
            parts = re.split(r"(?<=[。！？!?])\s*", normalized_line)
            sentences.extend(part.strip() for part in parts if part.strip())
        return sentences

    def _message_replyer_candidate_sentences(self, message: Any) -> List[str]:
        """从一条历史消息中提取可注入给回复器选择的候选句。"""

        if not isinstance(message, dict):
            return []
        if message.get("is_command") or message.get("is_notify") or message.get("is_emoji"):
            return []

        raw_message = message.get("raw_message")
        if isinstance(raw_message, list):
            text = "\n".join(self._raw_text_segments(message)).strip()
        else:
            text = self._message_text(message)
        if not text:
            return []

        candidates: List[str] = []
        for sentence in self._split_text_sentences(text):
            normalized_sentence = self._normalize_repeat_text(sentence)
            is_valid, _ = self._validate_repeat_text(normalized_sentence)
            if not is_valid or self._extract_emoji_description(normalized_sentence) is not None:
                continue
            candidates.append(normalized_sentence)
        return candidates

    async def _pick_replyer_candidate_sentences(self, chat_id: str) -> List[str]:
        """从当前聊天历史中随机抽取回复器候选句。"""

        injection_config = self.config.replyer_injection
        recent_messages = await self._get_recent_messages_from_chat(
            chat_id,
            limit=injection_config.history_limit,
            filter_mai=True,
        )

        candidates: List[str] = []
        seen_texts: set[str] = set()
        for message in recent_messages:
            for sentence in self._message_replyer_candidate_sentences(message):
                if sentence in seen_texts:
                    continue
                seen_texts.add(sentence)
                candidates.append(sentence)

        random.shuffle(candidates)
        return candidates[: injection_config.candidate_count]

    @staticmethod
    def _extract_prompt_message_text(message: Dict[str, Any]) -> str:
        """提取 Context Item 消息中的文本内容。"""

        parts = message.get("parts")
        if not isinstance(parts, list):
            return ""
        return "".join(
            part["text"]
            for part in parts
            if isinstance(part, dict) and part.get("type") == "text" and isinstance(part.get("text"), str)
        )

    @staticmethod
    def _has_replyer_injection_marker(messages: List[Dict[str, Any]]) -> bool:
        """判断本次 prompt 是否已经注入过牛牛候选句提示。"""

        return any(
            NIUNIU_REPLYER_INJECTION_MARKER in NiuniuPlugin._extract_prompt_message_text(message)
            for message in messages
        )

    @staticmethod
    def _build_replyer_injection_prompt(sentences: List[str]) -> str:
        """构造一次性 user message 提示。"""

        lines = [
            NIUNIU_REPLYER_INJECTION_MARKER,
            "下面是从当前聊天历史里随机抽取的候选句。请先在心里比较它们和当前对话的气氛，从中选一句作为本次回复内容。",
            "最终只输出所选原句，不要解释、不要编号、不要改写，也不要添加前后缀。",
            "候选句：",
        ]
        lines.extend(f"{index}. {sentence}" for index, sentence in enumerate(sentences, start=1))
        return "\n".join(lines)

    def _inject_replyer_injection_prompt(self, messages: Any, sentences: List[str]) -> Any:
        """向 replyer 本次请求末尾追加牛牛候选句提示。"""

        if not sentences or not isinstance(messages, list):
            return messages

        normalized_messages = [dict(message) for message in messages if isinstance(message, dict)]
        if len(normalized_messages) != len(messages) or self._has_replyer_injection_marker(normalized_messages):
            return messages

        injection_message = {
            "item_type": "UserMessageItem",
            "meta": {
                "item_id": uuid4().hex,
                "logical_turn_id": None,
                "timestamp": datetime.now().isoformat(),
            },
            "parts": [{"type": "text", "text": self._build_replyer_injection_prompt(sentences)}],
        }
        return [*normalized_messages, injection_message]

    @staticmethod
    def _segment_base64(segment: Dict[str, Any]) -> str:
        """历史图片消息通常只保存媒体 hash，这里通过图片管理器回查文件。"""

        segment_type = str(segment.get("type") or "").strip().lower()
        media_hash = NiuniuPlugin._segment_hash(segment)
        if not media_hash:
            return ""

        try:
            media_path: Optional[Path] = None
            if segment_type == "image":
                from src.chat.image_system.image_manager import image_manager

                image = image_manager.get_image_from_db(media_hash)
                media_path = image.full_path if image is not None else None

            if media_path is None or not media_path.is_file():
                return ""
            return base64.b64encode(media_path.read_bytes()).decode("utf-8")
        except Exception:
            return ""

    @staticmethod
    def _segment_hash(segment: Dict[str, Any]) -> str:
        """提取历史媒体消息段里的 hash 字段。"""

        return str(segment.get("hash") or "").strip()

    @staticmethod
    def _append_sendable_segment(segments: List[Tuple[str, str]], segment: Dict[str, Any]) -> bool:
        """把一个原始消息段追加为可发送段，仅允许文本和图片。"""

        segment_type = str(segment.get("type") or "").strip().lower()
        if segment_type == "text":
            text = str(segment.get("data") or segment.get("content") or "").strip()
            if text:
                segments.append(("text", text))
            return True

        if segment_type == "image":
            media_base64 = NiuniuPlugin._segment_base64(segment)
            if not media_base64:
                return False
            segments.append((segment_type, media_base64))
            return True

        return True

    @staticmethod
    def _raw_message_segments(message: Dict[str, Any]) -> List[Tuple[str, str]]:
        """将原始消息段转换成可按顺序发送的段。"""

        raw_message = message.get("raw_message")
        if not isinstance(raw_message, list):
            return []

        segments: List[Tuple[str, str]] = []
        for segment in raw_message:
            if not isinstance(segment, dict):
                continue
            if not NiuniuPlugin._append_sendable_segment(segments, segment):
                return []
        return segments

    async def _send_message(self, message: Dict[str, Any], chat_id: str) -> bool:
        """发送抽中的历史消息。"""

        raw_segments = await asyncio.to_thread(self._raw_message_segments, message)
        has_raw_message = isinstance(message.get("raw_message"), list)
        if has_raw_message and not raw_segments:
            self.ctx.logger.info(
                "麦麦牛牛跳过不包含文本或图片的消息: message_id=%s",
                str(message.get("message_id") or ""),
            )
            return False

        if raw_segments:
            sent_any = False
            for segment_type, content in raw_segments:
                if segment_type == "text":
                    await self.ctx.send.text(
                        content,
                        chat_id,
                        sync_to_maisaka_history=True,
                        maisaka_source_kind="plugin_send",
                    )
                    sent_any = True
                    continue
                if segment_type == "image":
                    await self.ctx.send.image(
                        content,
                        chat_id,
                        sync_to_maisaka_history=True,
                        maisaka_source_kind="plugin_send",
                    )
                    sent_any = True
                    continue
            if sent_any:
                return True

        text = self._message_text(message)
        if not text:
            return False
        await self.ctx.send.text(
            text,
            chat_id,
            sync_to_maisaka_history=True,
            maisaka_source_kind="plugin_send",
        )
        return True

    @staticmethod
    def _normalize_repeat_text(text: str) -> str:
        """归一化复读文本，避免首尾空白导致重复检测失效。"""

        return "\n".join(line.strip() for line in text.strip().splitlines()).strip()

    def _validate_repeat_text(self, text: str) -> Tuple[bool, str]:
        """校验 LLM 选择的复读文本是否适合直接发送。"""

        rule = self.config.rule
        text_length = len(text)
        if text_length < rule.min_message_length:
            return False, "复读内容太短。"
        if text_length > rule.max_message_length:
            return False, "复读内容太长。"
        if text.startswith(("/", "!", "！")):
            return False, "不会复读命令类消息。"
        return True, ""

    @staticmethod
    def _extract_emoji_description(text: str) -> Optional[str]:
        """从上下文渲染的表情描述中提取检索关键词。"""

        normalized_text = text.strip()
        if not normalized_text:
            return None

        patterns = (
            r"^\[表情[包]?[：:]\s*(?P<description>[^\]]+)\]$",
            r"^表情[包]?[：:]\s*(?P<description>.+)$",
        )
        for pattern in patterns:
            match = re.match(pattern, normalized_text)
            if match is None:
                continue
            description = match.group("description").strip()
            return description or None

        if normalized_text in {"[表情]", "[表情包]", "表情", "表情包"}:
            return "表情包"
        return None

    def _is_in_repeat_cooldown(self, stream_id: str, text: str) -> bool:
        """检查同一会话同一内容是否仍处于冷却期。"""

        cooldown_seconds = self.config.rule.cooldown_seconds
        if cooldown_seconds <= 0:
            return False

        now = time.time()
        key = (stream_id, text)
        last_repeat_at = self._last_repeat_at.get(key, 0.0)
        if now - last_repeat_at < cooldown_seconds:
            return True
        self._last_repeat_at[key] = now
        return False

    @HookHandler(
        "maisaka.replyer.before_model_request",
        name="maimai_niuniu_replyer_random_sentence_choice",
        description="按概率向 replyer 注入当前聊天历史候选句，让模型从中选择一句作为回复。",
        mode=HookMode.BLOCKING,
        order=HookOrder.LATE,
        timeout_ms=3000,
        error_policy=ErrorPolicy.SKIP,
    )
    async def inject_replyer_random_sentence_choice(self, **kwargs: Any) -> Dict[str, Any]:
        """在 replyer 最终请求中按概率追加一次性的牛牛候选句提示。"""

        modified_kwargs = dict(kwargs)
        injection_config = self.config.replyer_injection
        if not self.config.plugin.enabled or not injection_config.enabled:
            return {"action": "continue", "modified_kwargs": modified_kwargs}

        try:
            retry_count = int(modified_kwargs.get("retry_count") or 0)
        except (TypeError, ValueError):
            retry_count = 0
        if retry_count > 0:
            return {"action": "continue", "modified_kwargs": modified_kwargs}

        session_id = str(modified_kwargs.get("session_id") or "").strip()
        messages = modified_kwargs.get("items")
        if not session_id or not isinstance(messages, list):
            return {"action": "continue", "modified_kwargs": modified_kwargs}

        probability = injection_config.probability
        if probability <= 0.0 or (probability < 1.0 and random.random() >= probability):
            return {"action": "continue", "modified_kwargs": modified_kwargs}
        if await self._latest_chat_message_is_self(session_id, ""):
            return {"action": "continue", "modified_kwargs": modified_kwargs}

        sentences = await self._pick_replyer_candidate_sentences(session_id)
        if not sentences:
            return {"action": "continue", "modified_kwargs": modified_kwargs}

        injected_messages = self._inject_replyer_injection_prompt(messages, sentences)
        if injected_messages is messages:
            return {"action": "continue", "modified_kwargs": modified_kwargs}

        modified_kwargs["items"] = injected_messages
        self.ctx.logger.info(
            "麦麦牛牛已向 replyer 注入随机候选句提示: chat_id=%s candidate_count=%s",
            session_id,
            len(sentences),
        )
        return {"action": "continue", "modified_kwargs": modified_kwargs}

    @Command(
        "niuniu_once",
        description="立即触发一次麦麦牛牛随机历史消息发送",
        pattern=r"^/(?:niuniu|牛牛)(?:\s+once)?$",
    )
    async def handle_niuniu_once(self, stream_id: str = "", **kwargs: Any) -> tuple[bool, str, bool]:
        """手动触发当前群聊的牛牛复读。"""

        group = await self._build_group_choice_from_context(
            stream_id=str(stream_id or ""),
            platform=str(kwargs.get("platform") or ""),
            group_id=str(kwargs.get("group_id") or ""),
        )
        if group is None:
            return False, "当前会话缺少 stream_id，无法牛牛复读", True

        sent = await self._pick_from_group(group)
        return sent, "麦麦牛牛已发送一条历史消息" if sent else "麦麦牛牛没有找到可发送的历史消息", True

    @Tool(
        "repeat_after_duplicates",
        description=(
            "当当前聊天上下文中已经有多条完全相同的文本消息时使用。"
            "不要用于只有一条消息、语义相似但文本不同、命令类消息或表情包。"
        ),
        parameters=[
            ToolParameterInfo(
                name="context",
                param_type=ToolParamType.STRING,
                description="要复读的原文，应当来自当前上下文中已经重复出现的同一条消息",
                required=True,
            ),
        ],
    )
    async def handle_repeat_after_duplicates(
        self,
        context: str = "",
        stream_id: str = "",
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """发送 LLM 从上下文中选出的复读文本。"""

        if not self.config.plugin.enabled:
            return {"success": False, "content": "麦麦牛牛插件已禁用，无法复读。"}
        if not stream_id:
            return {"success": False, "content": "缺少当前会话 stream_id，无法复读。"}

        try:
            if await self._latest_chat_message_is_self(stream_id, str(kwargs.get("platform") or "")):
                return {"success": False, "content": "最新一条消息来自麦麦自己，跳过复读。"}

            repeated_text = self._normalize_repeat_text(context)
            is_valid, error_message = self._validate_repeat_text(repeated_text)
            if not is_valid:
                return {"success": False, "content": error_message}
            if self._is_in_repeat_cooldown(stream_id, repeated_text):
                return {"success": False, "content": "这条内容刚刚复读过，仍在冷却中。"}

            emoji_description = self._extract_emoji_description(repeated_text)
            if emoji_description is not None:
                return {"success": False, "content": "麦麦牛牛不会复读表情包。"}

            await self.ctx.send.text(
                repeated_text,
                stream_id,
                sync_to_maisaka_history=True,
                maisaka_source_kind="plugin_send",
            )
            return {
                "success": True,
                "content": f"已复读：{repeated_text}",
                "repeated_text": repeated_text,
            }
        except Exception as exc:
            self.ctx.logger.info("复读工具调用失败：stream_id=%s error=%s", stream_id, exc, exc_info=True)
            return {"success": False, "content": f"复读失败：{exc}"}


def create_plugin() -> NiuniuPlugin:
    """创建插件实例。"""

    return NiuniuPlugin()
