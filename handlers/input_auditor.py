"""真实输入构成审计（在 ``BEFORE_LLM_REQUEST`` 上）。

**为什么需要它**：``prompt_audit`` 挂在 ``ON_PROMPT_BUILD``，只能看见模板占位符
（实测那部分只占单次输入 **1.7%**）。而完整输入在别处：

- ``tools``：**工具与动作的 JSON Schema**——实测单次请求约 59k token，
  其中约 41k 属于「system prompt + 工具声明」这个固定块，量到底哪块占比最大，
  只能在这里分开数；
- ``payloads``：按角色（system / tool / user / assistant / tool_result）分开数，
  这样「对话历史」和「固定开销」才分得开。

框架明确支持订阅者修改 ``payloads`` / ``tools`` / ``stream`` 三个字段
（见 ``request_execution.py`` 的注释），所以**这里量出来的结果直接对应
「能不能裁、裁了省多少」**——本 handler 只读不改。

只读、不改任何字段、永远返回 ``SUCCESS``。
"""

from __future__ import annotations

import time
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseEventHandler
from src.app.plugin_system.types import EventType
from src.kernel.event import EventDecision

from .. import state as state_module

logger = get_logger("context_archiver.input_auditor")


def _chars_of(obj: Any, depth: int = 0) -> int:
    """尽力估算一个对象的文本长度（工具 schema / payload 内容都用它）。

    Args:
        obj: 任意对象。
        depth: 递归深度保护。

    Returns:
        字符数。
    """
    if obj is None or depth > 5:
        return 0
    if isinstance(obj, str):
        return len(obj)
    if isinstance(obj, (int, float, bool)):
        return len(str(obj))
    if isinstance(obj, (list, tuple, set)):
        return sum(_chars_of(item, depth + 1) for item in obj)
    if isinstance(obj, dict):
        return sum(len(str(k)) + _chars_of(v, depth + 1) for k, v in obj.items())

    text = getattr(obj, "text", None)
    if isinstance(text, str):
        return len(text)

    # 工具声明 / 内容对象：优先按「会被序列化出去的东西」算
    for attr in ("content", "schema", "parameters", "description", "name"):
        value = getattr(obj, attr, None)
        if value is not None and not callable(value):
            if isinstance(value, str):
                return len(value)
            return _chars_of(value, depth + 1)

    try:
        return len(str(obj))
    except Exception:  # noqa: BLE001 - 算不出来就当 0
        return 0


class InputAuditorHandler(BaseEventHandler):
    """逐次记录「这一次请求到底发了什么」。"""

    name: str = "input_auditor"
    description: str = "统计每次 LLM 请求的输入构成：工具声明 vs 各角色 payload"
    weight: int = 9999
    intercept_message: bool = False
    init_subscribe: list[EventType | str] = [EventType.BEFORE_LLM_REQUEST]

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """处理 ``before_llm_request``：只统计，不改任何字段。"""
        config = self.plugin.config
        if not (config.plugin.enabled and config.audit.input_audit_enabled):
            return EventDecision.SUCCESS, params

        request_name = str(params.get("request_name") or "")
        allowed = {
            str(name) for name in (config.audit.input_audit_request_names or []) if str(name)
        }
        if allowed and request_name not in allowed:
            return EventDecision.SUCCESS, params

        try:
            tools = params.get("tools")
            payloads = params.get("payloads")
            tool_list = list(tools) if isinstance(tools, (list, tuple)) else []
            payload_list = list(payloads) if isinstance(payloads, (list, tuple)) else []

            tools_chars = sum(_chars_of(tool) for tool in tool_list)

            roles: dict[str, int] = {}
            payload_chars = 0
            for payload in payload_list:
                role = str(getattr(payload, "role", "?") or "?")
                content = getattr(payload, "content", payload)
                chars = _chars_of(content)
                roles[role] = roles.get(role, 0) + chars
                payload_chars += chars

            record = {
                "at": time.time(),
                "request_name": request_name,
                "tools_count": len(tool_list),
                "tools_chars": tools_chars,
                "payload_chars": payload_chars,
                "total_chars": tools_chars + payload_chars,
                "roles": roles,
            }
            await state_module.record_input_audit(record)

            if config.plugin.debug_log:
                logger.info(
                    f"[context_archiver] 输入构成：tools={len(tool_list)} 个/{tools_chars} 字符，"
                    f"payloads={payload_chars} 字符 {roles}"
                )
        except Exception as error:  # noqa: BLE001 - 统计失败绝不能影响请求
            logger.warning(f"[context_archiver] 输入构成统计失败: {error}")

        return EventDecision.SUCCESS, params


__all__ = ["InputAuditorHandler"]
