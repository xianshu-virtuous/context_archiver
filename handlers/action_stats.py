"""动作/工具调用统计（为省钱服务）。

**为什么要它**：实测一次「对话回合」平均触发 **5.56 次** LLM 请求，
而**每多一步就多花 0.0350 元**（一整次带完整上下文的重发）。
想省钱就必须知道这 5.56 步花在哪些动作上——框架没把 action 落盘
（实例的 ``action_records`` 表是空的），所以只能自己数。

只读统计：不拦截、不改参数、永远返回 ``SUCCESS``。
"""

from __future__ import annotations

import time
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseEventHandler
from src.app.plugin_system.types import EventType
from src.kernel.event import EventDecision

from .. import state as state_module

logger = get_logger("context_archiver.action_stats")

#: 订阅的事件（动作与工具都数，字段名不同所以做兼容）。
_WATCHED: list[EventType | str] = [
    EventType.AFTER_ACTION_CALL,
    EventType.AFTER_TOOL_CALL,
]

#: 取名字时按顺序试的字段。
_NAME_KEYS: tuple[str, ...] = ("action_name", "tool_name", "name", "signature")


def _name_of(params: dict[str, Any]) -> str:
    """从事件参数里取动作/工具名。"""
    for key in _NAME_KEYS:
        value = params.get(key)
        if value:
            return str(value)
    return ""


class ActionStatsHandler(BaseEventHandler):
    """统计每个动作/工具被调用了多少次。"""

    name: str = "action_stats"
    description: str = "统计动作与工具调用次数（定位 agent 循环的 5.56 步花在哪）"
    weight: int = 1
    intercept_message: bool = False
    init_subscribe: list[EventType | str] = _WATCHED

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """处理动作/工具调用事件。"""
        config = self.plugin.config
        if not (config.plugin.enabled and config.audit.action_stats_enabled):
            return EventDecision.SUCCESS, params

        try:
            name = _name_of(params)
            if not name:
                return EventDecision.SUCCESS, params

            # 事件名归一：把 "after_action_call" 之类压成 action/tool
            kind = "tool" if "tool" in str(event_name).lower() else "action"
            stream_id = ""
            message = params.get("message")
            if message is not None:
                stream_id = str(getattr(message, "stream_id", "") or "")[:16]

            success = params.get("success")
            await state_module.record_action(
                name=name,
                kind=kind,
                stream_id=stream_id,
                success=bool(success) if success is not None else True,
                at=time.time(),
            )
        except Exception as error:  # noqa: BLE001 - 统计失败绝不能影响调用链
            logger.warning(f"[context_archiver] 动作统计失败: {error}")

        return EventDecision.SUCCESS, params


__all__ = ["ActionStatsHandler"]
