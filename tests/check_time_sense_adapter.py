"""context_archiver × time_sense 2.0 适配自检。

用法（用 neo 实例自己的 venv python 跑）::

    $env:NEO_ROOT="F:\\Neo-MoFox-Aemeath"
    & "F:\\Neo-MoFox-Aemeath\\.venv\\Scripts\\python.exe" F:\\mofox插件\\plugin\\repo\\context_archiver\\tests\\check_time_sense_adapter.py

只验 ``global_quiet_ok`` 这一处与外部插件的接口——它决定「别的流在不在聊」：

1. 默认（``require_global_quiet=False``）根本不查，零副作用；
2. time_sense 2.0：能按流查「本流安静多久」并列出**别的流**，
   「本流在聊」与「别处在聊」是两条不同的判据；
3. time_sense 1.x（没有 ``capabilities()``）：退回全局时间戳口径，文案与从前一致；
4. 服务缺失 / 查询抛错 / 老版本不认参数：一律**放行**（降级，绝不因外部插件挡住归档）。
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

_ENV_ROOT = os.environ.get("NEO_ROOT", "").strip()
PLUGIN_DIR = Path(__file__).resolve().parent.parent
NEO_ROOT = Path(_ENV_ROOT) if _ENV_ROOT else PLUGIN_DIR.parent.parent

sys.path.insert(0, str(NEO_ROOT))
sys.path.insert(0, str(PLUGIN_DIR.parent))

_failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    """打印一条检查结果。"""
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f"  — {detail}" if detail else ""))
    if not condition:
        _failures.append(label)


def section(title: str) -> None:
    """打印分节标题。"""
    print("-" * 72)
    print(title)


class _V2Sense:
    """time_sense 2.0 风格服务替身。"""

    def __init__(
        self,
        *,
        own_seconds: float = 3600.0,
        global_seconds: float = 3600.0,
        overview: list[dict] | None = None,
        overview_boom: bool = False,
        own_has_record: bool = True,
    ) -> None:
        self.own_seconds = own_seconds
        self.global_seconds = global_seconds
        self.overview = overview if overview is not None else []
        self.overview_boom = overview_boom
        self.own_has_record = own_has_record
        self.own_calls: list[str] = []

    def capabilities(self) -> dict:
        return {
            "plugin": "time_sense",
            "version": "2.0.0",
            "features": {"per_stream_clock": True, "gap_semantics": True},
            "events": {"first_message_after_gap": "time_sense:first_message_after_gap"},
        }

    async def since_last_message(self, stream_id: str = "") -> dict:
        self.own_calls.append(stream_id)
        if stream_id:
            return {
                "has_record": self.own_has_record,
                "seconds": self.own_seconds,
                "text": f"{int(self.own_seconds)}s",
                "source": "stream",
            }
        return {
            "has_record": True,
            "seconds": self.global_seconds,
            "text": f"{int(self.global_seconds)}s",
            "source": "global",
        }

    async def streams_overview(self, limit: int = 20) -> list[dict]:
        if self.overview_boom:
            raise RuntimeError("overview down")
        return self.overview


class _V1Sense:
    """time_sense 1.x 风格服务替身（没有 capabilities，也不认 stream_id）。"""

    def __init__(self, *, global_seconds: float = 3600.0) -> None:
        self.global_seconds = global_seconds

    async def since_last_message(self) -> dict:
        return {"has_record": True, "seconds": self.global_seconds, "text": f"{int(self.global_seconds)}s"}


async def main() -> int:
    """入口。

    Returns:
        退出码（0 = 全部通过）。
    """
    from context_archiver import archiver
    from context_archiver.config import ContextArchiverConfig

    print(f"框架根  : {NEO_ROOT}  存在={NEO_ROOT.exists()}")
    print(f"插件目录: {PLUGIN_DIR}")
    print("-" * 72)

    original = archiver.service_api.get_service

    def _use(service: object) -> None:
        archiver.service_api.get_service = lambda signature: service  # type: ignore[assignment]

    try:
        # ── 1. 默认不查 ─────────────────────────────────────────────────────
        section("1. 默认配置：不做全局静默校验（零副作用）")
        config = ContextArchiverConfig()
        config.trigger.require_global_quiet = False
        _use(_V2Sense(own_seconds=5.0))
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("默认关闭时直接放行", ok and reason == "未启用全局静默校验", reason)

        config.trigger.require_global_quiet = True
        config.trigger.idle_seconds = 30

        # ── 2. v2 按流判定 ──────────────────────────────────────────────────
        section("2. time_sense 2.0：本流 / 别处 分开判")

        own_active = _V2Sense(own_seconds=10.0, overview=[])
        _use(own_active)
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("本流刚有人说话 → 不放行", not ok and "本流" in reason, reason)
        check("本流判定用的是按流接口", own_active.own_calls == ["s1"], str(own_active.own_calls))

        foreign_active = _V2Sense(
            own_seconds=600.0,
            overview=[
                {"stream_id": "s1", "silence_seconds": 600.0},
                {"stream_id": "s2-abcdef", "silence_seconds": 3.0},
            ],
        )
        _use(foreign_active)
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check(
            "本流安静但别处在聊 → 不放行",
            not ok and "别处仍在活动" in reason and "s2-abcde" in reason,
            reason,
        )

        all_quiet = _V2Sense(
            own_seconds=600.0,
            overview=[
                {"stream_id": "s1", "silence_seconds": 600.0},
                {"stream_id": "s2", "silence_seconds": 900.0},
            ],
        )
        _use(all_quiet)
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("本流与其它流都安静 → 放行", ok and "按流校验通过" in reason, reason)

        only_self = _V2Sense(own_seconds=600.0, overview=[{"stream_id": "s1", "silence_seconds": 600.0}])
        _use(only_self)
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("只有自己这一条流 → 放行", ok and "按流校验通过" in reason, reason)

        no_record = _V2Sense(own_seconds=0.0, overview=[], own_has_record=False)
        _use(no_record)
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("本流没有记录时不误挡（仍按别处判）", ok and "按流校验通过" in reason, reason)

        # ── 3. v1 兜底 ──────────────────────────────────────────────────────
        section("3. time_sense 1.x：退回全局口径")

        _use(_V1Sense(global_seconds=5.0))
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check(
            "1.x：全局有活动 → 不放行且文案标注老版本口径",
            (not ok) and "仍有活动" in reason and "老版本" in reason,
            reason,
        )

        _use(_V1Sense(global_seconds=600.0))
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check(
            "1.x：全局静默 → 放行且文案标注老版本口径",
            ok and "全局（time_sense 老版本口径）静默" in reason,
            reason,
        )

        # ── 4. 降级路径 ─────────────────────────────────────────────────────
        section("4. 各种异常都放行（外部插件绝不挡住归档）")

        archiver.service_api.get_service = lambda signature: None  # type: ignore[assignment]
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("没有 time_sense → 放行", ok and "没有 time_sense" in reason, reason)

        def _boom(signature):
            raise RuntimeError("service api down")

        archiver.service_api.get_service = _boom  # type: ignore[assignment]
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("查询服务抛异常 → 放行", ok and "不可用" in reason, reason)

        _use(_V2Sense(own_seconds=600.0, overview_boom=True))
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("流清单查询失败 → 放行并说明", ok and "按流查询失败" in reason, reason)

        class _LyingSense:
            """声称有 capabilities，却不认 stream_id（模拟把签名搞混的实现）。"""

            def capabilities(self) -> dict:
                return {"version": "2.0.0", "features": {}}

            async def since_last_message(self) -> dict:
                return {"has_record": True, "seconds": 600.0}

            async def streams_overview(self) -> list[dict]:
                return []

        _use(_LyingSense())
        ok, reason = await archiver.global_quiet_ok(config, stream_id="s1")
        check("老实现混进新签名 → 退回全局口径放行", ok, reason)

        # ── 5. 没给 stream_id 时仍可用 ──────────────────────────────────────
        section("5. 没给 stream_id：按全局判，不崩")
        _use(_V2Sense(global_seconds=600.0))
        ok, reason = await archiver.global_quiet_ok(config)
        check("无 stream_id 时走全局口径", ok and "全局静默" in reason, reason)
        _use(_V2Sense(global_seconds=1.0))
        ok, reason = await archiver.global_quiet_ok(config)
        check("无 stream_id 且全局活跃 → 不放行", not ok and "全局仍有活动" in reason, reason)
    finally:
        archiver.service_api.get_service = original  # type: ignore[assignment]

    # ── 6. 目标模板一致性 ───────────────────────────────────────────────────
    section("6. 目标模板名单与 time_sense 对齐")
    from context_archiver.handlers import activity_tracker, prompt_auditor

    for label, targets in (
        ("activity_tracker", activity_tracker._TARGET_PROMPTS),
        ("prompt_auditor", prompt_auditor._TARGET_PROMPTS),
    ):
        check(
            f"{label} 覆盖 default / neo_default / kfc",
            {"default_chatter_user_prompt", "neo_default_chatter_user_prompt", "kfc_user_prompt"}
            <= set(targets),
            str(sorted(targets)),
        )
    default_targets = set(ContextArchiverConfig().recall.target_prompts)
    check(
        "recall.target_prompts 默认含 kfc_user_prompt",
        "kfc_user_prompt" in default_targets,
        str(sorted(default_targets)),
    )

    print("-" * 72)
    if _failures:
        print(f"结果：{len(_failures)} 项失败 -> {_failures}")
        return 1
    print("结果：全部通过")
    return 0


def _no_record_dict() -> dict:
    """没有记录时的 ``since_last_message`` 返回（供人工排查时复用）。"""
    return {"has_record": False, "seconds": 0.0, "text": ""}


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
