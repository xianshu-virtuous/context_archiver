"""活跃追踪与结束信号。

这一层只做两件事，都很轻：

1. **记活跃**：谁在什么时候说过话（按流分开记）。框架里没有现成的 per-stream
   活跃时间可读（``ChatStreams.last_active_time`` 在库里但没开 API），
   所以自己记一份，顺便对外做成 service，别的插件也能用。
2. **记结束信号**：``stop_conversation`` 就是「这一轮结束了」的官方语义
   （``pass_and_wait`` 是「挂起、准备恢复」，**绝不能当结束**）。

外加一个注入器：清空上下文之后，把该流的滚动摘要当成背景塞回 prompt，
这就是「清空但不忘事」的落点。
"""

from __future__ import annotations

import time
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseEventHandler
from src.app.plugin_system.types import EventType
from src.kernel.event import EventDecision

from .. import archiver
from .. import state as state_module

logger = get_logger("context_archiver.tracker")

#: 摘要注入的目标模板名（两个 chatter 都兼容，只会有实际启用的那个命中）。
_TARGET_PROMPTS: frozenset[str] = frozenset(
    {"default_chatter_user_prompt", "neo_default_chatter_user_prompt"}
)

#: 注入块的标题与边界说明。写清「这是背景不是台词」，降低被整句搬走的概率。
_INJECT_TEMPLATE = """## 你记得的这段对话
{summary}
- 以上是你自己早前留下的记忆摘要，不是要你念出来的台词，也不要复述原句或照搬措辞。
- 细节可能已经模糊：如果对话里需要确切的原文，用记忆/上下文查询工具去找，不要凭摘要编造。"""


def _stream_id_of(message: Any) -> str:
    """尽力取出消息所属聊天流标识。

    不同适配器的 ``Message`` 字段不完全一致，按候选名逐个试，取不到返回空串。

    Args:
        message: 消息对象。

    Returns:
        聊天流标识；取不到时为空字符串。
    """
    if message is None:
        return ""
    for attr in ("stream_id", "session_id", "chat_stream_id"):
        value = getattr(message, attr, None)
        if value:
            return str(value)
    stream = getattr(message, "chat_stream", None)
    return str(getattr(stream, "stream_id", "") or "")


class ActivityTrackerHandler(BaseEventHandler):
    """记录每流的活跃时刻与结束信号。

    只写自己的状态，不改消息、不拦截事件——所以永远返回 ``SUCCESS``，
    消息分发链路上还有别的订阅者，这里绝不能吃掉事件。
    """

    name: str = "activity_tracker"
    description: str = "记录聊天流活跃时刻、累计待归档消息数，并识别 stop_conversation 结束信号"
    weight: int = 0
    intercept_message: bool = False
    init_subscribe: list[EventType | str] = [
        EventType.ON_MESSAGE_RECEIVED,
        EventType.ON_MESSAGE_SENT,
        EventType.AFTER_ACTION_CALL,
    ]

    async def _touch(
        self,
        stream_id: str,
        *,
        kind: str,
        count_message: bool,
        engage: bool = False,
    ) -> None:
        """刷新某个流的活跃状态。

        Args:
            stream_id: 聊天流标识。
            kind: 活跃来源（写状态与日志用）。
            count_message: 是否计入待归档消息数（只有真正的消息事件才计）。
            engage: 是否算「Bot 自己参与」（只有 Bot 发言才算）。
        """
        if not stream_id:
            return
        stream_state = await state_module.ArchiveStateStore.get(stream_id)
        stream_state.last_activity_at = time.time()
        stream_state.last_activity_kind = kind
        if engage:
            # 判定基准：Bot 潜水多久了。群里别人一直说话不会刷新它。
            stream_state.last_engagement_at = stream_state.last_activity_at
            stream_state.last_engagement_kind = kind
        if count_message:
            stream_state.pending_count = int(stream_state.pending_count or 0) + 1

        states = await state_module.ArchiveStateStore.load_all()
        states[stream_id] = stream_state
        await state_module.ArchiveStateStore.save_all(states)

    async def _mark_end_signal(self, stream_id: str, action_name: str) -> None:
        """记录一次结束信号。"""
        if not stream_id:
            return
        stream_state = await state_module.ArchiveStateStore.get(stream_id)
        stream_state.end_signal_at = time.time()
        stream_state.end_signal_name = action_name
        stream_state.last_activity_at = stream_state.end_signal_at
        # Bot 主动结束对话，也算它参与了这一轮。
        stream_state.last_engagement_at = stream_state.end_signal_at
        stream_state.last_engagement_kind = f"end:{action_name}"

        states = await state_module.ArchiveStateStore.load_all()
        states[stream_id] = stream_state
        await state_module.ArchiveStateStore.save_all(states, force=True)

        if self.plugin.config.plugin.debug_log:
            logger.info(
                f"[context_archiver] 收到结束信号 {action_name}（stream={stream_id[:8]}）"
            )

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """处理订阅到的事件。

        Args:
            event_name: 事件名。
            params: 事件参数。

        Returns:
            ``(EventDecision.SUCCESS, params)``，不改动任何参数。
        """
        config = self.plugin.config
        if not config.plugin.enabled:
            return EventDecision.SUCCESS, params

        try:
            if event_name == EventType.AFTER_ACTION_CALL:
                action_name = str(params.get("action_name") or "")
                if action_name in state_module.END_ACTION_NAMES:
                    stream_id = _stream_id_of(params.get("message"))
                    if stream_id:
                        await self._mark_end_signal(stream_id, action_name)
                elif action_name in state_module.WAIT_ACTION_NAMES:
                    # 挂起 ≠ 结束：只刷新活跃，绝不当结束信号。
                    stream_id = _stream_id_of(params.get("message"))
                    await self._touch(stream_id, kind=f"action:{action_name}", count_message=False)
                return EventDecision.SUCCESS, params

            message = params.get("message")
            stream_id = _stream_id_of(message)
            kind = (
                "message_received"
                if event_name == EventType.ON_MESSAGE_RECEIVED
                else "message_sent"
            )
            # 只有 Bot 自己发消息才算「参与」——别人之间说话只刷新活跃。
            await self._touch(
                stream_id,
                kind=kind,
                count_message=True,
                engage=(kind == "message_sent"),
            )
        except Exception as error:  # noqa: BLE001 - 记录失败绝不能影响消息链路
            logger.warning(f"[context_archiver] 活跃追踪失败: {error}")

        return EventDecision.SUCCESS, params


class ArchiveSummaryInjector(BaseEventHandler):
    """清空上下文之后，把该流的滚动摘要注入回 prompt。

    注入位置是 user prompt 的 ``values["extra"]``（跟在历史消息之后），
    而不是 system reminder —— 因为摘要是**按流**的，
    而 ``on_prompt_build`` 的参数里没有 stream_id，只有 ``values`` 里带着它。

    只有 ``archive.clear_context_enabled`` 打开时才需要注入：
    没清空的话历史本来就在，再塞一份摘要是浪费 token。
    """

    name: str = "archive_summary_injector"
    description: str = "把归档摘要注入 user prompt 的 extra 板块（清空上下文后用于保持记忆连续）"
    weight: int = 20
    intercept_message: bool = False
    init_subscribe: list[EventType | str] = [EventType.ON_PROMPT_BUILD]

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """处理 ``on_prompt_build``。"""
        config = self.plugin.config
        if not (config.plugin.enabled and config.archive.inject_summary_after_clear):
            return EventDecision.SUCCESS, params
        if not config.archive.clear_context_enabled:
            return EventDecision.SUCCESS, params
        if str(params.get("name") or "") not in _TARGET_PROMPTS:
            return EventDecision.SUCCESS, params

        values = params.get("values")
        if not isinstance(values, dict):
            return EventDecision.SUCCESS, params

        stream_id = str(values.get("stream_id") or "").strip()
        if not stream_id:
            return EventDecision.SUCCESS, params

        try:
            stream_state = await state_module.ArchiveStateStore.get(stream_id)
        except Exception as error:  # noqa: BLE001 - 读不到就跳过注入
            logger.warning(f"[context_archiver] 读取摘要失败: {error}")
            return EventDecision.SUCCESS, params

        summary = str(stream_state.summary or "").strip()
        if not summary:
            return EventDecision.SUCCESS, params

        block = _INJECT_TEMPLATE.format(summary=summary)
        existing = str(values.get("extra") or "").strip()
        values["extra"] = f"{existing}\n\n{block}" if existing else block
        params["values"] = values

        if config.plugin.debug_log:
            logger.info(
                f"[context_archiver] 已注入归档摘要（stream={stream_id[:8]}, {len(summary)} 字）"
            )
        return EventDecision.SUCCESS, params


__all__ = ["ActivityTrackerHandler", "ArchiveSummaryInjector"]
