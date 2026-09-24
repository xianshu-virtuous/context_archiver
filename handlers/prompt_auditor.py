"""提示词注入归因审计。

**它要回答的问题**：每轮 8,561 token 的全价输入（占实测成本 74%）到底是哪来的？
账本已经证明它与历史长度无关（`corr(输入体积, miss) = +0.14`），
那它只能来自「每轮都在变的内容」——也就是各插件塞进 prompt 的东西。

**怎么做**：挂在 ``ON_PROMPT_BUILD`` 上，`weight` 取**最大**（最后执行，
看到所有插件注入完的最终值），把 user prompt 模板里各占位符的字符数记下来。

只看不改：不动 `values`、不拦截、永远返回 ``SUCCESS``。
"""

from __future__ import annotations

import time
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseEventHandler
from src.app.plugin_system.types import EventType
from src.kernel.event import EventDecision

from .. import state as state_module

logger = get_logger("context_archiver.prompt_auditor")

#: 审计的目标模板（与摘要注入器保持一致，只会有实际启用的那个命中）。
_TARGET_PROMPTS: frozenset[str] = frozenset(
    {"default_chatter_user_prompt", "neo_default_chatter_user_prompt"}
)

#: 需要单独统计的占位符。其余键合并计入 ``other``。
_TRACKED_KEYS: tuple[str, ...] = ("history", "unreads", "extra", "extra_info")


def _length_of(value: Any) -> int:
    """尽力取文本长度（非字符串按 0 计）。"""
    if isinstance(value, str):
        return len(value)
    if isinstance(value, (list, tuple)):
        return sum(len(v) for v in value if isinstance(v, str))
    return 0


class PromptAuditorHandler(BaseEventHandler):
    """逐轮记录 user prompt 的构成，用于成本归因。

    ``weight`` 特意取大值：事件处理器按权重升序执行，取最大可以保证
    本处理器看到的是**所有注入完成之后**的最终文本。
    """

    name: str = "prompt_auditor"
    description: str = "记录每轮 user prompt 各板块的字符数，回答「钱花在哪」"
    weight: int = 9999
    intercept_message: bool = False
    init_subscribe: list[EventType | str] = [EventType.ON_PROMPT_BUILD]

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """处理 ``on_prompt_build``：只统计，不改动任何内容。"""
        config = self.plugin.config
        if not (config.plugin.enabled and config.audit.prompt_audit_enabled):
            return EventDecision.SUCCESS, params
        if str(params.get("name") or "") not in _TARGET_PROMPTS:
            return EventDecision.SUCCESS, params

        values = params.get("values")
        if not isinstance(values, dict):
            return EventDecision.SUCCESS, params

        try:
            tracked = {key: _length_of(values.get(key)) for key in _TRACKED_KEYS}
            total = sum(_length_of(v) for v in values.values())

            record = {
                "at": time.time(),
                "stream_id": str(values.get("stream_id") or "")[:16],
                "history": tracked["history"],
                "unreads": tracked["unreads"],
                "extra": tracked["extra"],
                "extra_info": tracked["extra_info"],
                "other": max(0, total - sum(tracked.values())),
                "total": total,
            }
            await state_module.record_prompt_audit(record)

            if config.plugin.debug_log:
                logger.info(
                    f"[context_archiver] prompt 构成：history={record['history']} "
                    f"unreads={record['unreads']} extra={record['extra']} "
                    f"other={record['other']} 合计={record['total']} 字符"
                )
        except Exception as error:  # noqa: BLE001 - 统计失败绝不能影响 prompt 构建
            logger.warning(f"[context_archiver] 提示词归因统计失败: {error}")

        return EventDecision.SUCCESS, params


__all__ = ["PromptAuditorHandler"]
