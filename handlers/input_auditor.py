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

import hashlib
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


def _text_of(obj: Any, depth: int = 0) -> str:
    """尽力把一个 payload 的内容拼成文本（算指纹用）。"""
    if obj is None or depth > 5:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, (int, float, bool)):
        return str(obj)
    if isinstance(obj, (list, tuple, set)):
        return "".join(_text_of(item, depth + 1) for item in obj)
    if isinstance(obj, dict):
        return "".join(f"{k}{_text_of(v, depth + 1)}" for k, v in obj.items())
    text = getattr(obj, "text", None)
    if isinstance(text, str):
        return text
    for attr in ("content", "schema", "parameters", "description", "name"):
        value = getattr(obj, attr, None)
        if value is not None and not callable(value):
            if isinstance(value, str):
                return value
            return _text_of(value, depth + 1)
    try:
        return str(obj)
    except Exception:  # noqa: BLE001
        return ""


def _tool_name(tool: Any) -> str:
    """尽力取工具/动作的名字（dict 或对象都吃）。"""
    if isinstance(tool, dict):
        for key in ("name", "tool_name", "function_name", "action_name"):
            value = tool.get(key)
            if value:
                return str(value)
        function = tool.get("function")
        if isinstance(function, dict) and function.get("name"):
            return str(function["name"])
        return ""
    for attr in ("name", "tool_name", "action_name"):
        value = getattr(tool, attr, None)
        if value:
            return str(value)
    return ""


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

            # ⚠️ 工具声明会**同时**出现在 tools 参数和 payloads 的 ROLE.TOOL 里，
            # 是同一批东西。直接相加会把工具声明算两遍（实测 66,866 被记成 133,732）。
            # 所以：payloads 里已有 TOOL 角色时，就以 payloads 为准。
            tool_in_payloads = any(
                "TOOL" in str(role).upper() and "RESULT" not in str(role).upper()
                for role in roles
            )
            total_chars = payload_chars if tool_in_payloads else payload_chars + tools_chars

            record = {
                "at": time.time(),
                "request_name": request_name,
                "tools_count": len(tool_list),
                "tools_chars": tools_chars,
                "payload_chars": payload_chars,
                "total_chars": total_chars,
                "roles": roles,
                # 按请求名分组计数：这样即使在同一个统计里混着多种调用也分得清
                "req": {request_name or "(未命名)": 1},
            }
            await state_module.record_input_audit(record)

            # 记录 payload 指纹：有了「每轮每个 payload 的角色/长度/内容哈希」，
            # 就能离线算出**第一个内容变化出现在第几个 payload** —— 那是缓存失效的起点，
            # 也是「把易变内容挪到末尾」这件事到底值多少钱的依据。
            fingerprints: list[dict[str, Any]] = []
            for payload in payload_list:
                role = str(getattr(payload, "role", "?") or "?")
                text = _text_of(getattr(payload, "content", payload))
                entry: dict[str, Any] = {
                    "r": role[-14:],
                    "n": len(text),
                    "h": hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()[:12]
                    if text
                    else "",
                }
                # payload 级的哈希分不出「变化出现在它内部的哪个位置」。
                # USER 里混着历史（稳定）+ 新消息与注入（易变），所以再记前若干个字符的
                # 分段哈希：哪一段开始不同，就说明缓存从那里断掉、后面全废。
                # 若前 2000 字符就变了 → 注入插在了历史前面，历史被冲掉（有肉可吃）；
                # 若只有整段哈希不同、前 32000 都一样 → 变化在尾部，已经是最优布局。
                if text and "USER" in role.upper():
                    for mark in (2000, 8000, 32000):
                        if len(text) >= mark:
                            entry[f"p{mark}"] = hashlib.sha1(
                                text[:mark].encode("utf-8", "ignore")
                            ).hexdigest()[:12]
                fingerprints.append(entry)
            await state_module.append_fingerprints(
                {"at": record["at"], "req": request_name, "fp": fingerprints}
            )

            # 顺手记一份「这次到底暴露了哪些工具」——要决定砍谁，先得知道有什么。
            names = sorted({_tool_name(tool) for tool in tool_list} - {""})
            if names:
                await state_module.save_tool_names(names)

            if config.plugin.debug_log:
                logger.info(
                    f"[context_archiver] 输入构成：tools={len(tool_list)} 个/{tools_chars} 字符，"
                    f"payloads={payload_chars} 字符 {roles}"
                )
        except Exception as error:  # noqa: BLE001 - 统计失败绝不能影响请求
            logger.warning(f"[context_archiver] 输入构成统计失败: {error}")

        return EventDecision.SUCCESS, params


__all__ = ["InputAuditorHandler"]
