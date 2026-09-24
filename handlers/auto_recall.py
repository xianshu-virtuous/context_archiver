"""读取侧注入器：不需要 Bot 主动查，就能把该记得的塞进上下文。

它是「绕过 tool 直接存取」的落点——

- **Bot 侧**：一次记忆工具都不用调（省掉一整轮 agent 循环，约 0.0350 元）；
- **插件侧**：在 ``on_prompt_build`` 里直接读 booku 的元数据层（默认路线零网络、毫秒级）。

冷却做在**进程内类属性**上：同一条记忆在 ``recall.cooldown_seconds`` 内不重复注入。
不做冷却的话，同一段事会被每轮重新塞进去——那是纯浪费，还会把上下文撑肿。
"""

from __future__ import annotations

import time
from typing import Any

from src.app.plugin_system.api import stream_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseEventHandler
from src.app.plugin_system.types import EventType
from src.kernel.event import EventDecision

from .. import recall as recall_module
from .. import state as state_module

logger = get_logger("context_archiver.auto_recall")


async def _person_id_of(stream_id: str) -> str:
    """取该聊天流的对话者 id（人物路要用）。"""
    if not stream_id:
        return ""
    try:
        info = await stream_api.get_stream_info(stream_id)
    except Exception:  # noqa: BLE001 - 拿不到就不走人物路
        return ""
    if isinstance(info, dict):
        for key in ("person_id", "user_id", "target_id"):
            value = info.get(key)
            if value:
                return str(value)
    return ""


class AutoRecallInjector(BaseEventHandler):
    """把召回结果注入 user prompt 的 extra 板块。

    ``weight`` 取 30：排在摘要注入器（20）之后，保证两段内容都进 extra 而不互相覆盖。
    """

    name: str = "auto_recall_injector"
    description: str = "自动召回记忆并注入 user prompt（不需要 Bot 主动调记忆工具）"
    weight: int = 30
    intercept_message: bool = False
    init_subscribe: list[EventType | str] = [EventType.ON_PROMPT_BUILD]

    #: 进程内冷却表：memory_id -> 最近注入时刻。框架每次新建 handler 实例，
    #: 所以必须挂类属性，否则冷却等于没有。
    _recent: dict[str, float] = {}

    #: 累计统计（给命令看）。
    _stats: dict[str, Any] = {"rounds": 0, "injected": 0, "chars": 0, "last_error": ""}

    def _prune(self, cooldown: int) -> None:
        """清理过期冷却项。"""
        if cooldown <= 0:
            AutoRecallInjector._recent.clear()
            return
        now = time.time()
        expired = [mid for mid, ts in self._recent.items() if now - ts >= cooldown]
        for memory_id in expired:
            self._recent.pop(memory_id, None)

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """处理 ``on_prompt_build``。"""
        config = self.plugin.config
        if not (config.plugin.enabled and config.recall.enabled):
            return EventDecision.SUCCESS, params
        if str(params.get("name") or "") not in set(config.recall.target_prompts):
            return EventDecision.SUCCESS, params

        values = params.get("values")
        if not isinstance(values, dict):
            return EventDecision.SUCCESS, params

        stream_id = str(values.get("stream_id") or "").strip()
        cooldown = int(config.recall.cooldown_seconds)
        self._prune(cooldown)

        try:
            person_id = await _person_id_of(stream_id)
            outcome = await recall_module.recall_for_prompt(
                config,
                values,
                exclude_ids=set(self._recent.keys()),
                person_id=person_id,
            )
        except Exception as error:  # noqa: BLE001 - 召回失败绝不能影响 prompt 构建
            AutoRecallInjector._stats["last_error"] = f"{type(error).__name__}: {error}"
            logger.warning(f"[context_archiver] 自动召回失败: {error}")
            return EventDecision.SUCCESS, params

        AutoRecallInjector._stats["rounds"] = int(AutoRecallInjector._stats["rounds"]) + 1
        if outcome.error:
            AutoRecallInjector._stats["last_error"] = outcome.error
        if not outcome.block:
            return EventDecision.SUCCESS, params

        existing = str(values.get("extra") or "").strip()
        values["extra"] = f"{existing}\n\n{outcome.block}" if existing else outcome.block
        params["values"] = values

        now = time.time()
        for memory_id in outcome.injected_ids:
            self._recent[memory_id] = now

        stats = AutoRecallInjector._stats
        stats["injected"] = int(stats["injected"]) + len(outcome.injected_ids)
        stats["chars"] = int(stats["chars"]) + outcome.chars
        stats["last_error"] = ""

        if config.plugin.debug_log:
            logger.info(
                f"[context_archiver] 自动召回注入 {len(outcome.injected_ids)} 条 "
                f"（{outcome.chars} 字符，来源 {outcome.sources}，stream={stream_id[:8]}）"
            )

        # 顺带记一笔到落盘统计（命令要看）
        await state_module.record_recall(len(outcome.injected_ids), outcome.chars, outcome.sources)
        return EventDecision.SUCCESS, params

    @classmethod
    def snapshot(cls) -> dict[str, Any]:
        """给命令看的进程内统计。"""
        return dict(cls._stats)

    @classmethod
    def recent_ids(cls) -> list[str]:
        """当前处于冷却中的记忆 id。"""
        return list(cls._recent.keys())


__all__ = ["AutoRecallInjector"]
