"""context_archiver 插件入口。

上下文归档器：**话题结束时把这段对话总结进记忆，然后清空该流上下文**，
让 Bot 不用每轮背着整段历史说话。

- **判定**：优先认 ``stop_conversation`` 结束信号；没有信号时用闲置时长兜底；
  装了 ``time_sense`` 就用它的「距上次说话」交叉验证全局是否也安静
  （**软依赖**：manifest 里不写它，取不到就降级）；
- **总结**：上次摘要 + 增量消息 → 新摘要 + 若干记忆条目（增量，不是重压全量）；
- **落记忆**：写进 ``booku_memory``，失败降级到本地 json，绝不静默丢；
- **清空**：只在记忆落地成功之后，且需要用户显式打开开关；
- **默认 observer**：先观察几天再动手——清空写的是持久水位线，不可逆。
"""

from __future__ import annotations

import asyncio
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BasePlugin, register_plugin
from src.kernel.concurrency import get_task_manager
from src.kernel.scheduler import TriggerType, get_unified_scheduler

from . import archiver
from .commands import ArchiveCommand
from .config import ContextArchiverConfig
from .handlers import ActivityTrackerHandler, ArchiveSummaryInjector, PromptAuditorHandler
from .service import ContextArchiverService

logger = get_logger("context_archiver")

#: 巡检任务的调度器任务名。
_TICK_TASK_NAME = "context_archiver_tick"

#: 巡检间隔的下限（秒），防止配置写太小把调度器打满。
_MIN_TICK_SECONDS = 10

#: 等调度器就绪的最大轮数（每轮 0.5s）。
_SCHEDULER_WAIT_ROUNDS = 600


def _tick_interval(config: Any) -> int:
    """读取巡检间隔（秒），非法值回退到 60，且不低于下限。"""
    raw: Any = 60
    trigger = getattr(config, "trigger", None)
    if trigger is not None:
        raw = getattr(trigger, "tick_seconds", raw)
    try:
        seconds = int(raw)
    except (TypeError, ValueError):
        seconds = 60
    return max(_MIN_TICK_SECONDS, seconds)


@register_plugin
class ContextArchiverPlugin(BasePlugin):
    """上下文归档器插件。"""

    plugin_name: str = "context_archiver"
    plugin_description: str = (
        "话题结束时把对话总结进记忆并清空该流上下文："
        "优先识别 stop_conversation 结束信号，配合 time_sense 判断空闲；默认只观察"
    )
    plugin_version: str = "1.0.0"
    configs: list[type] = [ContextArchiverConfig]

    def __init__(self, config: Any = None) -> None:
        """初始化插件。

        Args:
            config: 插件配置实例。
        """
        super().__init__(config)
        self._schedule_ids: list[str] = []
        self._register_task_id: str | None = None

    def get_components(self) -> list[type]:
        """返回本插件提供的组件。

        插件被关掉时不注册任何组件——等于整体下线，连活跃都不记。

        Returns:
            组件类列表。
        """
        if isinstance(self.config, ContextArchiverConfig) and not self.config.plugin.enabled:
            return []
        return [
            ContextArchiverService,
            ActivityTrackerHandler,
            ArchiveSummaryInjector,
            PromptAuditorHandler,
            ArchiveCommand,
        ]

    # ── 生命周期 ────────────────────────────────────────────────────────────

    async def on_plugin_loaded(self) -> None:
        """加载后：排队注册巡检任务（调度器要等 Bot.start() 才可用）。"""
        if isinstance(self.config, ContextArchiverConfig) and not self.config.plugin.enabled:
            return

        try:
            task_info = get_task_manager().create_task(
                self._register_tick_when_ready(),
                name="context_archiver_tick_register",
                daemon=True,
            )
            self._register_task_id = task_info.task_id
        except Exception as error:  # noqa: BLE001 - 注册失败只影响归档，不影响对话
            logger.warning(f"[context_archiver] 排队注册巡检任务失败: {error}")

    async def on_plugin_unloaded(self) -> None:
        """卸载前：取消注册任务并移除巡检调度。

        每一步各自兜底——调度器可能根本没启动。
        """
        if self._register_task_id:
            try:
                get_task_manager().cancel_task(self._register_task_id)
            except Exception:  # noqa: BLE001 - 任务可能已结束
                pass
            self._register_task_id = None

        schedule_ids = list(self._schedule_ids)
        self._schedule_ids.clear()

        try:
            scheduler = get_unified_scheduler()
        except Exception:  # noqa: BLE001 - 调度器不可用时无需清理
            scheduler = None
        if scheduler is None:
            return

        try:
            found = await scheduler.find_schedule_by_name(_TICK_TASK_NAME)
        except Exception:  # noqa: BLE001
            found = None
        if found and found not in schedule_ids:
            schedule_ids.append(found)

        for schedule_id in schedule_ids:
            try:
                await scheduler.remove_schedule(schedule_id)
            except Exception as error:  # noqa: BLE001 - 卸载阶段尽力清理
                logger.debug(f"[context_archiver] 移除巡检任务失败: {error}")

    # ── 巡检注册与执行 ──────────────────────────────────────────────────────

    async def _register_tick_when_ready(self) -> None:
        """等调度器就绪后注册周期巡检。

        ``scheduler.start()`` 发生在 Bot 运行阶段，插件加载时调度器还没起，
        所以这里轮询等待，拿到 ``RuntimeError`` 就退避重试。
        """
        interval = _tick_interval(self.config)
        try:
            scheduler = get_unified_scheduler()
        except Exception as error:  # noqa: BLE001 - 拿不到调度器就别注册
            logger.warning(f"[context_archiver] 调度器不可用，巡检未注册: {error}")
            return

        for _ in range(_SCHEDULER_WAIT_ROUNDS):
            try:
                schedule_id = await scheduler.create_schedule(
                    callback=self._tick_job,
                    trigger_type=TriggerType.TIME,
                    trigger_config={"interval_seconds": interval},
                    is_recurring=True,
                    task_name=_TICK_TASK_NAME,
                    force_overwrite=True,
                )
            except RuntimeError:
                await asyncio.sleep(0.5)
                continue
            except Exception as error:  # noqa: BLE001 - 反复失败就放弃注册
                logger.warning(f"[context_archiver] 注册巡检任务失败: {error}")
                await asyncio.sleep(2.0)
                continue

            self._schedule_ids = [schedule_id]
            mode = getattr(getattr(self.config, "plugin", None), "mode", "observer")
            logger.info(f"[context_archiver] 巡检已注册（每 {interval}s，模式 {mode}）")
            return

        logger.warning("[context_archiver] 等待调度器就绪超时，巡检未注册")

    async def _tick_job(self) -> None:
        """调度器回调：跑一次巡检。"""
        config = self.config
        if not isinstance(config, ContextArchiverConfig):
            return
        if not config.plugin.enabled:
            return

        try:
            actions = await archiver.tick(config)
        except Exception as error:  # noqa: BLE001 - 巡检失败不打断调度
            logger.error(f"[context_archiver] 巡检异常: {error}")
            return

        if not actions:
            return
        if config.plugin.debug_log or str(config.plugin.mode).lower() != "observer":
            for action in actions:
                logger.info(f"[context_archiver] 巡检动作: {action}")


__all__ = ["ContextArchiverPlugin"]
