"""context_archiver 服务组件。

对外提供「这段对话归档到哪了」的只读视图与手动触发入口，供命令、
WebUI 或别的插件调用。签名：``context_archiver:service:context_archiver``。

注意框架语义：``service_api.get_service()`` **每次调用都新建实例**，
所以进程内共享的东西一律放类属性或落盘（见 ``state.ArchiveStateStore``）。
"""

from __future__ import annotations

import time
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseService

from . import archiver
from . import state as state_module
from .config import ContextArchiverConfig

logger = get_logger("context_archiver.service")


class ContextArchiverService(BaseService):
    """上下文归档服务。

    对外方法：

    - ``status``：插件整体状态（模式、开关、累计归档数）
    - ``overview``：各流待归档概览（活跃时间、待归档条数、是否有结束信号）
    - ``stream_state``：单个流的状态
    - ``audit``：最近的归档审计记录
    - ``archive_now``：立刻归档某个流（可 dry_run 演练）
    - ``run_tick``：手动跑一次巡检
    """

    name: str = "context_archiver"
    description: str = "聊天流上下文的归档与清空服务"

    @property
    def config(self) -> ContextArchiverConfig:
        """插件配置。"""
        return self.plugin.config

    async def status(self) -> dict[str, Any]:
        """插件整体状态摘要。"""
        states = await state_module.ArchiveStateStore.load_all()
        cfg = self.config
        total_archives = sum(int(s.archive_count or 0) for s in states.values())
        pending_total = sum(int(s.pending_count or 0) for s in states.values())
        return {
            "mode": str(cfg.plugin.mode or "observer"),
            "enabled": bool(cfg.plugin.enabled),
            "clear_context_enabled": bool(cfg.archive.clear_context_enabled),
            "sink": str(cfg.memory.sink or "booku"),
            "tick_seconds": int(cfg.trigger.tick_seconds),
            "idle_seconds": int(cfg.trigger.idle_seconds),
            "settle_seconds": int(cfg.trigger.settle_seconds),
            "min_messages": int(cfg.trigger.min_messages),
            "tracked_streams": len([sid for sid in states if sid]),
            "pending_messages": pending_total,
            "total_archives": total_archives,
        }

    async def overview(self) -> list[dict[str, Any]]:
        """各流的待归档概览（按最近活跃倒序）。"""
        states = await state_module.ArchiveStateStore.load_all()
        now = time.time()
        rows: list[dict[str, Any]] = []
        for stream_id, stream_state in states.items():
            if not stream_id:
                continue
            last_activity = float(stream_state.last_activity_at or 0.0)
            rows.append(
                {
                    "stream_id": stream_id,
                    "pending_count": int(stream_state.pending_count or 0),
                    "idle_seconds": max(0.0, now - last_activity) if last_activity else 0.0,
                    "end_signal": bool(
                        stream_state.end_signal_at
                        and stream_state.end_signal_at > stream_state.last_settled_signal_at
                    ),
                    "archive_count": int(stream_state.archive_count or 0),
                    "last_archive_at": float(stream_state.last_archive_at or 0.0),
                    "summary_chars": len(str(stream_state.summary or "")),
                    "last_activity_kind": str(stream_state.last_activity_kind or ""),
                }
            )
        rows.sort(key=lambda item: item["idle_seconds"])
        return rows

    async def stream_state(self, stream_id: str) -> dict[str, Any] | None:
        """单个流的状态（不存在时返回 ``None``）。"""
        states = await state_module.ArchiveStateStore.load_all()
        stream_state = states.get(str(stream_id or ""))
        if stream_state is None:
            return None
        now = time.time()
        last_activity = float(stream_state.last_activity_at or 0.0)
        decision = archiver.evaluate(stream_state, self.config, now=now)
        return {
            "stream_id": stream_state.stream_id,
            "pending_count": int(stream_state.pending_count or 0),
            "last_activity_at": last_activity,
            "idle_seconds": max(0.0, now - last_activity) if last_activity else 0.0,
            "end_signal_at": float(stream_state.end_signal_at or 0.0),
            "end_signal_name": stream_state.end_signal_name,
            "waterline_ts": float(stream_state.waterline_ts or 0.0),
            "archive_count": int(stream_state.archive_count or 0),
            "summary": stream_state.summary,
            "summary_updated_at": float(stream_state.summary_updated_at or 0.0),
            "decision": {
                "should": decision.should,
                "reason": decision.reason,
                "trigger": decision.trigger,
            },
        }

    async def audit(self, limit: int = 20) -> list[dict[str, Any]]:
        """最近的归档审计记录。"""
        records = await state_module.load_audit()
        return [record.to_dict() for record in records[: max(1, int(limit))]]

    async def prompt_audit(self) -> dict[str, Any]:
        """提示词构成的归因统计（回答「每轮的钱花在哪」）。

        先强制落盘缓冲，保证拿到的是最新数据。

        Returns:
            含 ``samples`` / ``avg_chars``（各板块平均字符数）的字典。
        """
        await state_module.flush_prompt_audit()
        data = await state_module.load_prompt_audit()
        samples = int(data.get("samples") or 0)
        raw_sums = data.get("sums")
        sums: dict[str, Any] = raw_sums if isinstance(raw_sums, dict) else {}

        fields = ("history", "unreads", "extra", "extra_info", "other", "total")
        avg_chars: dict[str, int] = {}
        for key in fields:
            try:
                avg_chars[key] = round(int(sums.get(key) or 0) / samples) if samples else 0
            except (TypeError, ValueError):
                avg_chars[key] = 0

        recent = data.get("recent")
        return {
            "samples": samples,
            "avg_chars": avg_chars,
            "first_at": float(data.get("first_at") or 0.0),
            "last_at": float(data.get("last_at") or 0.0),
            "recent": recent if isinstance(recent, list) else [],
        }

    async def recall_stats(self) -> dict[str, Any]:
        """自动召回的累计统计（注入多少条、多少字符、各路贡献）。

        Returns:
            含 ``rounds`` / ``injected`` / ``chars`` / ``sources`` 等字段的字典。
        """
        await state_module.flush_stats(force=True)
        counters = await state_module.load_recall_stats()
        rounds = int(counters.get("rounds") or 0)
        injected = int(counters.get("injected") or 0)
        chars = int(counters.get("chars") or 0)
        sources = {
            key[4:]: value for key, value in counters.items() if key.startswith("src:")
        }
        return {
            "enabled": bool(self.config.recall.enabled),
            "mode": str(self.config.recall.mode or "structured"),
            "top_k": int(self.config.recall.top_k),
            "cooldown_seconds": int(self.config.recall.cooldown_seconds),
            "rounds": rounds,
            "injected": injected,
            "chars": chars,
            "sources": sources,
            "avg_items_per_round": round(injected / rounds, 2) if rounds else 0.0,
            "avg_chars_per_round": round(chars / rounds) if rounds else 0,
        }

    async def action_stats(self) -> dict[str, Any]:
        """动作与工具的调用次数统计（用于定位 agent 循环的步数构成）。

        Returns:
            含 ``total`` / ``actions`` / ``tools`` 的字典（按次数倒序）。
        """
        await state_module.flush_stats(force=True)
        counters = await state_module.load_action_stats()

        def _pick(prefix: str) -> dict[str, int]:
            picked: dict[str, int] = {}
            for key, value in counters.items():
                if not key.startswith(prefix) or key.endswith(":failed"):
                    continue
                picked[key[len(prefix):]] = int(value)
            return dict(sorted(picked.items(), key=lambda item: -item[1]))

        actions = _pick("action:")
        tools = _pick("tool:")
        return {
            "total": sum(actions.values()) + sum(tools.values()),
            "actions": actions,
            "tools": tools,
        }

    async def input_audit(self) -> dict[str, Any]:
        """真实输入构成统计：工具声明 vs 各角色 payload。

        用来回答「那 41k 的固定开销里，工具声明占多少、砍哪块最值」。

        Returns:
            含 ``samples`` / ``avg_tools_chars`` / ``avg_payload_chars`` / ``roles`` 的字典。
        """
        await state_module.flush_input_audit()
        data = await state_module.load_input_audit()
        samples = int(data.get("samples") or 0)
        raw = data.get("counters")
        counters: dict[str, Any] = raw if isinstance(raw, dict) else {}

        def _avg(key: str) -> float:
            try:
                return round(int(counters.get(key) or 0) / samples) if samples else 0
            except (TypeError, ValueError):
                return 0

        roles = {
            key[len("role:"):]: _avg(key)
            for key in counters
            if str(key).startswith("role:")
        }
        return {
            "samples": samples,
            "avg_tools_count": _avg("tools_count"),
            "avg_tools_chars": _avg("tools_chars"),
            "avg_payload_chars": _avg("payload_chars"),
            "avg_total_chars": _avg("total_chars"),
            "roles": dict(sorted(roles.items(), key=lambda item: -item[1])),
            "recent": data.get("recent") if isinstance(data.get("recent"), list) else [],
        }

    async def tool_names(self) -> list[str]:
        """最近一次暴露给模型的全部工具名（「砍哪些」的决策输入）。"""
        return await state_module.load_tool_names()

    async def archive_now(
        self,
        stream_id: str,
        *,
        dry_run: bool = False,
        trigger: str = archiver.TRIGGER_MANUAL,
    ) -> dict[str, Any]:
        """立刻归档某个流。

        Args:
            stream_id: 聊天流标识。
            dry_run: 只总结、不落记忆、不清空（预演用）。
            trigger: 触发类型文案。

        Returns:
            归档快照字典。
        """
        snapshot = await archiver.archive_stream(
            stream_id,
            trigger=trigger,
            config=self.config,
            dry_run=dry_run,
            force=True,
        )
        return snapshot.to_dict()

    async def run_tick(self) -> list[dict[str, Any]]:
        """手动跑一次巡检（与定时任务同一入口）。"""
        return await archiver.tick(self.config)


__all__ = ["ContextArchiverService"]
