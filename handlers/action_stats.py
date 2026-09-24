"""动作/工具调用统计（为省钱服务）。

**为什么要它**：实测一次「对话回合」平均触发 **5.56 次** LLM 请求，
而**每多一步就多花 0.0350 元**（一整次带完整上下文的重发）。
想省钱就必须知道这 5.56 步花在哪些动作上——框架没把 action 落盘
（实例的 ``action_records`` 表是空的），所以只能自己数。

**为什么订阅 ``AFTER_CHATTER_STEP`` 而不是 ``AFTER_ACTION_CALL``**：
实测（2026-09-24 实例日志）default_chatter **不走 core 的 action_manager**——
整份日志里一条「动作执行完成」都没有，而 `AFTER_ACTION_CALL` 只在
``action_manager.execute`` 里发布。DFC 自己在 chatter 流程里处理工具调用，
但它每轮结束会发 ``AFTER_CHATTER_STEP``，参数里直接带本轮 ``used_tools``：::

    当前回合的工具调用: ['action-send_text', 'memory_command']

所以 ``AFTER_CHATTER_STEP`` 才是能拿到「这一步用了什么」的钩子。
``AFTER_ACTION_CALL`` / ``AFTER_TOOL_CALL`` 仍然订阅，供别的 chatter 用。

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

#: 订阅的事件。``AFTER_CHATTER_STEP`` 是主力（DFC 唯一能拿到 used_tools 的地方）。
_WATCHED: list[EventType | str] = [
    EventType.AFTER_CHATTER_STEP,
    EventType.AFTER_ACTION_CALL,
    EventType.AFTER_TOOL_CALL,
]

#: 取名字时按顺序试的字段。
_NAME_KEYS: tuple[str, ...] = ("action_name", "tool_name", "name", "signature")

#: 只统计这个作用域的 chatter 步（与 booku 的记忆工具告警一致）。
_TARGET_SCOPE = "actor_round"


def _split_call(raw: str) -> tuple[str, str]:
    """把 ``action-send_text`` / ``memory_command`` 拆成 (kind, name)。

    Args:
        raw: 原始调用名。

    Returns:
        ``(kind, name)``，kind 为 ``action`` 或 ``tool``。
    """
    text = str(raw or "").strip()
    if not text:
        return "", ""
    if text.startswith("action-"):
        return "action", text[len("action-"):]
    if text.startswith("tool-"):
        return "tool", text[len("tool-"):]
    return "tool", text


class ActionStatsHandler(BaseEventHandler):
    """统计每轮 chatter 用了哪些动作/工具，以及各被调用多少次。"""

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
        """处理 chatter 步进 / 动作 / 工具事件。"""
        config = self.plugin.config
        if not (config.plugin.enabled and config.audit.action_stats_enabled):
            return EventDecision.SUCCESS, params

        try:
            if str(event_name) == str(EventType.AFTER_CHATTER_STEP):
                await self._on_chatter_step(params)
            else:
                await self._on_single_call(event_name, params)
        except Exception as error:  # noqa: BLE001 - 统计失败绝不能影响调用链
            logger.warning(f"[context_archiver] 动作统计失败: {error}")

        return EventDecision.SUCCESS, params

    async def _on_chatter_step(self, params: dict[str, Any]) -> None:
        """一个 chatter 步结束：把这轮用到的工具全部记一笔。

        **不按 ``step_scope`` 过滤**：不同 chatter 的 scope 名不一样（实测见过
        ``actor_round``、也见过空串），限定反而会一条都收不到。
        改为把 scope 本身记进统计——跑一轮就知道实际有哪些值。
        """
        step_scope = str(params.get("step_scope") or "").strip() or "(空)"
        used_tools = params.get("used_tools")
        if not isinstance(used_tools, (list, tuple)):
            # 没有 used_tools 也记一笔，方便诊断「事件到底有没有来」
            await state_module.record_action(
                name="__no_used_tools__",
                kind="diag",
                scope=step_scope,
                at=time.time(),
            )
            return

        stream_id = str(params.get("stream_id") or "")[:16]
        now = time.time()
        recorded = False
        for raw in used_tools:
            kind, name = _split_call(str(raw))
            if not name:
                continue
            recorded = True
            await state_module.record_action(
                name=name,
                kind=kind,
                stream_id=stream_id,
                success=True,
                at=now,
                scope=step_scope,
            )
        if not recorded:
            await state_module.record_action(
                name="__empty_round__",
                kind="diag",
                scope=step_scope,
                at=now,
            )

    async def _on_single_call(self, event_name: str, params: dict[str, Any]) -> None:
        """单次 action / tool 事件（别的 chatter 可能走这条）。"""
        name = ""
        for key in _NAME_KEYS:
            value = params.get(key)
            if value:
                name = str(value)
                break
        if not name:
            return

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


__all__ = ["ActionStatsHandler"]
