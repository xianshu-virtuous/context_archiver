"""``/归档`` 命令：看清状态、手动触发、切换模式。

仅主人可用（``PermissionLevel.OWNER``）——它会触发真实的记忆写入，
清空开关打开时还会清上下文，不适合非主人调用。

```
/归档                 — 状态总览 + 各流概况
/归档 状态 /status    — 同上
/归档 审计 /audit     — 最近几次归档做了什么
/归档 流 /stream      — 当前流的详细状态与判定原因
/归档 演练 /dry       — 对当前流做一次「只总结不落盘」的预演
/归档 现在 /now       — 立刻归档当前流（真实写入）
/归档 模式 /mode      — 查看运行模式
/归档 observer|active — 切换运行模式（只改运行时，不回写 config.toml）
/归档 帮助 /help      — 帮助
```

模式切换只改**运行时**的配置对象、不回写 ``config.toml``（跟 daily_schedule
的离线生活同一原则）：重启后回到配置文件里的值，用户始终有最终解释权。
"""

from __future__ import annotations

import time
from typing import Any

from src.app.plugin_system.api import service_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.api.send_api import send_text
from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel

logger = get_logger("context_archiver.command")

#: 服务组件签名。
_SERVICE_SIGNATURE = "context_archiver:service:context_archiver"

_HELP = """【上下文归档器】
/归档                状态总览
/归档 状态           同上
/归档 审计           最近几次归档记录
/归档 注入           提示词构成归因（钱花在哪）
/归档 召回           自动召回统计（记起了多少、成本多少）
/归档 动作           动作/工具调用分布（agent 步数花在哪）
/归档 流             当前流的详细状态
/归档 演练           只总结不落盘（预演）
/归档 现在           立刻归档当前流
/归档 模式           查看运行模式
/归档 observer       切到观察模式（只记录不动作）
/归档 active         切到执行模式
/归档 帮助           本帮助"""


def _humanize(seconds: float) -> str:
    """秒 → 人话。"""
    total = max(0.0, float(seconds))
    if total < 60:
        return f"{int(total)}秒"
    if total < 3600:
        return f"{int(total // 60)}分钟"
    if total < 86400:
        return f"{total / 3600:.1f}小时"
    return f"{total / 86400:.1f}天"


class ArchiveCommand(BaseCommand):
    """上下文归档命令。"""

    name: str = "归档"
    description: str = "查看/触发上下文归档（总结进记忆并可选清空上下文）"
    permission_level: PermissionLevel = PermissionLevel.OWNER

    @classmethod
    def match(cls, parts: list[str]) -> int:
        """匹配命令名，支持 ``归档`` 与 ``archive`` 两种触发词。"""
        if not parts:
            return 0
        if parts[0] in ("归档", "archive"):
            return 1
        return 0

    async def _reply(self, text: str) -> None:
        """向当前聊天流发送文本回复。"""
        await send_text(text, stream_id=self.stream_id)

    def _get_service(self) -> Any:
        """取归档服务实例。"""
        return service_api.get_service(_SERVICE_SIGNATURE)

    async def _render_status(self) -> str:
        """拼状态总览文本。"""
        service = self._get_service()
        status = await service.status()
        rows = await service.overview()

        lines = [
            "【上下文归档器】",
            f"模式：{status['mode']}（observer 只观察，active 才动手）",
            f"清空上下文：{'开' if status['clear_context_enabled'] else '关'}",
            f"sink：{status['sink']}",
            f"判定：巡检 {status['tick_seconds']}s ｜ 闲置 {status['idle_seconds']}s ｜ "
            f"结束信号后等 {status['settle_seconds']}s ｜ 最少 {status['min_messages']} 条",
            f"累计归档：{status['total_archives']} 次 ｜ 跟踪 {status['tracked_streams']} 个流 "
            f"｜ 待归档 {status['pending_messages']} 条",
        ]

        if rows:
            lines.append("")
            lines.append("各流概览：")
            for row in rows[:6]:
                signal = "｜有结束信号" if row["end_signal"] else ""
                lines.append(
                    f"  {row['stream_id'][:8]} 待归档 {row['pending_count']} 条 ｜ "
                    f"静默 {_humanize(row['idle_seconds'])}{signal} ｜ "
                    f"已归档 {row['archive_count']} 次"
                )
        return "\n".join(lines)

    @cmd_route()
    async def handle_status(self) -> tuple[bool, str]:
        """状态总览。"""
        text = await self._render_status()
        await self._reply(text)
        return True, "status"

    @cmd_route("状态")
    async def handle_status_cn(self) -> tuple[bool, str]:
        """状态总览（中文别名）。"""
        return await self.handle_status()

    @cmd_route("status")
    async def handle_status_en(self) -> tuple[bool, str]:
        """状态总览（英文别名）。"""
        return await self.handle_status()

    @cmd_route("审计")
    async def handle_audit(self) -> tuple[bool, str]:
        """最近的归档审计记录。"""
        service = self._get_service()
        records = await service.audit(limit=8)
        if not records:
            await self._reply("还没有归档记录。")
            return True, "empty audit"

        lines = ["【最近归档】"]
        for item in records:
            when = time.strftime("%m-%d %H:%M", time.localtime(float(item.get("at") or 0.0)))
            flag = "✓" if item.get("ok") else "✗"
            cleared = "已清空" if item.get("cleared") else "未清空"
            start = time.strftime("%m-%d %H:%M", time.localtime(float(item.get("start_ts") or 0.0)))
            end = time.strftime("%m-%d %H:%M", time.localtime(float(item.get("end_ts") or 0.0)))
            lines.append(
                f"{flag} {when} {str(item.get('stream_id') or '')[:8]} "
                f"[{item.get('trigger')}] {item.get('message_count')} 条 "
                f"({start}~{end}) 记忆 {len(item.get('memory_ids') or [])} 条 {cleared}"
            )
            summary = str(item.get("summary") or "").strip()
            if summary:
                lines.append(f"   摘要：{summary[:60]}…")
            error = str(item.get("error") or "").strip()
            if error:
                lines.append(f"   错误：{error[:80]}")
        await self._reply("\n".join(lines))
        return True, "audit"

    @cmd_route("audit")
    async def handle_audit_en(self) -> tuple[bool, str]:
        """审计（英文别名）。"""
        return await self.handle_audit()

    @cmd_route("注入")
    async def handle_prompt_audit(self) -> tuple[bool, str]:
        """提示词构成归因：每轮的钱花在哪。"""
        service = self._get_service()
        data = await service.prompt_audit()
        samples = int(data.get("samples") or 0)
        if not samples:
            await self._reply("还没有采样——等 prompt 构建跑过几轮再看。")
            return True, "empty"

        avg = data.get("avg_chars") or {}

        def tok(chars: int) -> int:
            """字符数粗估 token（中文约 1 token ≈ 1.5 字符）。"""
            return int(chars / 1.5)

        lines = [
            f"【提示词构成归因】{samples} 轮采样",
            f"  history {avg.get('history', 0):>7,} 字符 ≈ {tok(avg.get('history', 0)):>6,} tok",
            f"  unreads {avg.get('unreads', 0):>7,} 字符 ≈ {tok(avg.get('unreads', 0)):>6,} tok",
            f"  extra   {avg.get('extra', 0):>7,} 字符 ≈ {tok(avg.get('extra', 0)):>6,} tok ← 各插件注入",
            f"  other   {avg.get('other', 0):>7,} 字符",
            f"  合计    {avg.get('total', 0):>7,} 字符 ≈ {tok(avg.get('total', 0)):>6,} tok",
            "",
            "对照：实测每轮「全价」输入约 8,561 tok（占总成本 74%），",
            "且与历史长度无关 → extra 越接近它，钱就越是花在插件注入上。",
        ]
        await self._reply("\n".join(lines))
        return True, "prompt-audit"

    @cmd_route("inject")
    async def handle_prompt_audit_en(self) -> tuple[bool, str]:
        """注入归因（英文别名）。"""
        return await self.handle_prompt_audit()

    @cmd_route("召回")
    async def handle_recall(self) -> tuple[bool, str]:
        """自动召回统计。"""
        service = self._get_service()
        data = await service.recall_stats()
        avg_chars = int(data.get("avg_chars_per_round") or 0)
        lines = [
            f"【自动召回】{'开' if data.get('enabled') else '关'}（路线 {data.get('mode')}）",
            f"采样 {data.get('rounds', 0)} 轮 ｜ 注入 {data.get('injected', 0)} 条 ｜ "
            f"{data.get('chars', 0):,} 字符",
            f"平均每轮 {data.get('avg_items_per_round', 0)} 条 / {avg_chars} 字符"
            f"（上限 {data.get('top_k', 0)} 条）",
            f"冷却 {data.get('cooldown_seconds', 0)}s",
        ]
        sources = data.get("sources") or {}
        if sources:
            detail = "、".join(
                f"{key} {value}" for key, value in sorted(sources.items(), key=lambda x: -x[1])
            )
            lines.append(f"各路候选：{detail}")
        lines.append("")
        lines.append("对照：省掉一次记忆 tool 调用 ≈ 0.0350 元/轮；")
        lines.append(
            f"注入成本 ≈ {avg_chars / 1.5 * 3.0 / 1e6:.5f} 元/轮（按全价粗估）——净赚。"
        )
        await self._reply("\n".join(lines))
        return True, "recall"

    @cmd_route("recall")
    async def handle_recall_en(self) -> tuple[bool, str]:
        """召回统计（英文别名）。"""
        return await self.handle_recall()

    @cmd_route("动作")
    async def handle_actions(self) -> tuple[bool, str]:
        """动作/工具调用分布。"""
        service = self._get_service()
        data = await service.action_stats()
        total = int(data.get("total") or 0)
        if not total:
            await self._reply("还没有动作统计——等 bot 跑过几轮再看。")
            return True, "empty"

        lines = [f"【动作/工具调用统计】合计 {total} 次"]
        actions = data.get("actions") or {}
        if actions:
            lines.append("动作：")
            for name, count in list(actions.items())[:10]:
                lines.append(f"  {str(name):<30}{count:>6}")
        tools = data.get("tools") or {}
        if tools:
            lines.append("工具：")
            for name, count in list(tools.items())[:10]:
                lines.append(f"  {str(name):<30}{count:>6}")
        lines.append("")
        lines.append("每次调用 = 一次 agent 步进 ≈ 0.0350 元。砍调用最多的那几个最划算。")
        await self._reply("\n".join(lines))
        return True, "actions"

    @cmd_route("actions")
    async def handle_actions_en(self) -> tuple[bool, str]:
        """动作统计（英文别名）。"""
        return await self.handle_actions()

    @cmd_route("流")
    async def handle_stream(self) -> tuple[bool, str]:
        """当前流的详细状态。"""
        service = self._get_service()
        state = await service.stream_state(self.stream_id)
        if not state:
            await self._reply("当前流还没有任何记录（本插件尚未观察到它的消息）。")
            return True, "no state"

        decision = state.get("decision") or {}
        lines = [
            f"【当前流 {str(self.stream_id)[:8]}】",
            f"待归档：{state['pending_count']} 条",
            f"静默：{_humanize(state['idle_seconds'])}",
            f"结束信号：{state['end_signal_name'] or '无'}",
            f"已归档：{state['archive_count']} 次（水位线 {state['waterline_ts']:.0f}）",
            f"判定：{'该归档' if decision.get('should') else '还不到时候'} —— {decision.get('reason')}",
        ]
        if state.get("summary"):
            lines.append(f"滚动摘要：{str(state['summary'])[:120]}…")
        await self._reply("\n".join(lines))
        return True, "stream"

    @cmd_route("stream")
    async def handle_stream_en(self) -> tuple[bool, str]:
        """当前流状态（英文别名）。"""
        return await self.handle_stream()

    @cmd_route("演练")
    async def handle_dry_run(self) -> tuple[bool, str]:
        """只总结不落盘、不清空。"""
        service = self._get_service()
        await self._reply("正在预演（不写记忆、不清空）…")
        result = await service.archive_now(self.stream_id, dry_run=True)
        if not result.get("ok"):
            await self._reply(f"预演失败/跳过：{result.get('error')}")
            return False, "dry failed"
        await self._reply(
            f"✓ 预演完成：{result['message_count']} 条消息\n摘要：\n{result.get('summary') or '（空）'}"
        )
        return True, "dry"

    @cmd_route("dry")
    async def handle_dry_run_en(self) -> tuple[bool, str]:
        """预演（英文别名）。"""
        return await self.handle_dry_run()

    @cmd_route("现在")
    async def handle_now(self) -> tuple[bool, str]:
        """立刻归档当前流（真实写入）。"""
        service = self._get_service()
        mode = str(self.plugin.config.plugin.mode or "observer").lower()
        if mode == "observer":
            await self._reply(
                "当前是 observer 模式，手动归档也会被拦下。\n"
                "先执行 /归档 active 切到执行模式（只改运行时，重启后回到配置文件的值）。"
            )
            return False, "observer mode"
        await self._reply("正在归档…")
        result = await service.archive_now(self.stream_id)
        if result.get("ok"):
            await self._reply(
                f"✓ 归档完成：{result['message_count']} 条消息 → "
                f"记忆 {len(result.get('memory_ids') or [])} 条（sink={result.get('sink')}）"
                f"{'，已清空上下文' if result.get('cleared') else '，未清空上下文'}"
            )
            return True, "archived"
        await self._reply(f"归档未完成：{result.get('error')}")
        return False, "archive failed"

    @cmd_route("now")
    async def handle_now_en(self) -> tuple[bool, str]:
        """立刻归档（英文别名）。"""
        return await self.handle_now()

    @cmd_route("模式")
    async def handle_mode(self) -> tuple[bool, str]:
        """查看运行模式。"""
        mode = str(self.plugin.config.plugin.mode or "observer")
        await self._reply(
            f"当前模式：{mode}\n"
            "observer —— 只观察记录；active —— 达到条件就归档。\n"
            "切换：/归档 observer 或 /归档 active"
        )
        return True, "mode"

    @cmd_route("mode")
    async def handle_mode_en(self) -> tuple[bool, str]:
        """查看运行模式（英文别名）。"""
        return await self.handle_mode()

    @cmd_route("observer")
    async def handle_set_observer(self) -> tuple[bool, str]:
        """切到观察模式。"""
        self.plugin.config.plugin.mode = "observer"
        await self._reply("✓ 已切到 observer（只观察记录，不总结、不写记忆、不清空）")
        logger.info("[context_archiver] 运行时模式切换为 observer")
        return True, "observer"

    @cmd_route("active")
    async def handle_set_active(self) -> tuple[bool, str]:
        """切到执行模式。"""
        self.plugin.config.plugin.mode = "active"
        clear_on = bool(self.plugin.config.archive.clear_context_enabled)
        await self._reply(
            "✓ 已切到 active（达到判定条件时会总结并写记忆）\n"
            f"清空上下文：{'开' if clear_on else '关'}\n"
            f"注意：本次切换只改运行时，重启后回到 config.toml 里的值。"
        )
        logger.info("[context_archiver] 运行时模式切换为 active")
        return True, "active"

    @cmd_route("帮助")
    async def handle_help(self) -> tuple[bool, str]:
        """帮助。"""
        await self._reply(_HELP)
        return True, "help"

    @cmd_route("help")
    async def handle_help_en(self) -> tuple[bool, str]:
        """帮助（英文别名）。"""
        return await self.handle_help()


__all__ = ["ArchiveCommand"]
