"""context_archiver 归档广播事件自检（``context_archiver:archived``）。

用法（用 neo 实例自己的 venv python 跑）::

    $env:NEO_ROOT="F:\\Neo-MoFox-Aemeath"
    & "F:\\Neo-MoFox-Aemeath\\.venv\\Scripts\\python.exe" F:\\mofox插件\\plugin\\repo\\context_archiver\\tests\\check_archive_event.py

只验「归档成功后广播」这一个出口——它是日记类插件「归档即联动」的落点。
不读盘、不调模型、不启动 Bot：

1. 成功 → 恰好发布一次，事件名与 payload 契约字段齐全；
2. 失败（``ok=False``）→ 一条都不发（水位线没推进，别把半截素材推出去）；
3. ``[events].published = false`` → 一条都不发；
4. 订阅方抛异常 → 被吞掉，**不影响归档本身**；
5. ``memory_ids`` 是快照的拷贝，订阅方改动不会回灌到归档结果。
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

# 有些控制台是 GBK：print 里带 ✓/⚠ 会抛 UnicodeEncodeError，
# 把「断言全过」变成 exit 1（踩过两次），所以先把输出改成 utf-8。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except Exception:  # noqa: BLE001 - 改不了就照旧
        pass

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


async def main() -> int:
    """跑完全部断言，返回进程退出码。"""
    from context_archiver import archiver
    from context_archiver.config import ContextArchiverConfig

    from src.app.plugin_system.api import event_api

    original_publish = event_api.publish_event
    published: list[tuple[str, dict]] = []

    async def _fake_publish(event, kwargs=None):
        published.append((str(event), dict(kwargs or {})))
        return {"decision": "success"}

    def _snapshot(**over):
        snap = archiver.StreamSnapshot(
            stream_id="stream-abc123",
            ok=True,
            trigger=archiver.TRIGGER_IDLE,
            message_count=12,
            start_ts=1760000000.0,
            end_ts=1760003600.0,
            summary="这段对话的摘要",
            memory_ids=["mem-1", "mem-2"],
            sink="booku",
            cleared=False,
        )
        for key, value in over.items():
            setattr(snap, key, value)
        return snap

    try:
        event_api.publish_event = _fake_publish  # type: ignore[assignment]
        config = ContextArchiverConfig()

        # ── 1. 成功路径 ────────────────────────────────────────────────────
        section("1. 归档成功 → 广播一次，payload 契约齐全")
        published.clear()
        await archiver._publish_archived_event(config, _snapshot(), summary="摘要正文")
        check("恰好发布一次", len(published) == 1, str(len(published)))
        name, payload = published[0] if published else ("", {})
        check("事件名是 context_archiver:archived", name == archiver.EVENT_ARCHIVED, name)
        required = {
            "event",
            "source",
            "timestamp",
            "stream_id",
            "at",
            "start_ts",
            "end_ts",
            "message_count",
            "summary",
            "memory_ids",
            "sink",
            "cleared",
            "truncated",
            "trigger",
            "error",
        }
        missing = sorted(required - set(payload))
        check("payload 字段一个不缺", not missing, f"缺: {missing}")
        check(
            "关键值对得上",
            payload.get("stream_id") == "stream-abc123"
            and payload.get("memory_ids") == ["mem-1", "mem-2"]
            and payload.get("source") == "context_archiver"
            and payload.get("sink") == "booku"
            and payload.get("trigger") == archiver.TRIGGER_IDLE,
            str({k: payload.get(k) for k in ("stream_id", "memory_ids", "source")}),
        )
        check("summary 用传入的那份", payload.get("summary") == "摘要正文")

        # ── 2. 失败路径 ────────────────────────────────────────────────────
        section("2. 归档失败 → 一条都不发")
        published.clear()
        await archiver._publish_archived_event(
            config, _snapshot(ok=False, error="总结调用失败"), summary="x"
        )
        check("ok=False 时不广播", not published, str(published))

        # ── 3. 开关 ────────────────────────────────────────────────────────
        section("3. [events].published = false → 一条都不发")
        muted = ContextArchiverConfig()
        muted.events.published = False
        published.clear()
        await archiver._publish_archived_event(muted, _snapshot(), summary="x")
        check("关掉广播后安静", not published, str(published))
        check(
            "默认是开着的（无订阅者零开销）",
            bool(ContextArchiverConfig().events.published) is True,
        )

        # ── 4. 订阅方炸了不能影响归档 ──────────────────────────────────────
        section("4. 订阅方抛异常 → 吞掉，不影响归档")
        async def _boom_publish(event, kwargs=None):
            raise RuntimeError("订阅方炸了")

        event_api.publish_event = _boom_publish  # type: ignore[assignment]
        swallowed = False
        try:
            await archiver._publish_archived_event(config, _snapshot(), summary="x")
            swallowed = True
        except Exception as error:  # noqa: BLE001 - 冒泡就是失败
            swallowed = False
            check("异常不该冒泡", False, repr(error))
        check("广播异常被吞掉（归档照常完成）", swallowed)

        # ── 5. 值语义 ──────────────────────────────────────────────────────
        section("5. payload 是快照的拷贝，订阅方改动不回灌")
        event_api.publish_event = _fake_publish  # type: ignore[assignment]
        published.clear()
        snap = _snapshot()
        await archiver._publish_archived_event(config, snap, summary="x")
        if published:
            published[0][1]["memory_ids"].append("被订阅方改了")
        check(
            "快照的 memory_ids 没被订阅方改动",
            snap.memory_ids == ["mem-1", "mem-2"],
            str(snap.memory_ids),
        )
    finally:
        event_api.publish_event = original_publish  # type: ignore[assignment]

    print("-" * 72)
    if _failures:
        print(f"结果：{len(_failures)} 项失败 -> {_failures}")
        return 1
    print("结果：全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
