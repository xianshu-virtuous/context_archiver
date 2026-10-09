"""context_archiver 记忆写入层（sink 适配）。

为什么单独一层：上游的记忆系统正在换代，插件不该把「写记忆」这件事
硬编码到某个具体插件上。这里只约定统一入参（``MemoryItem``）与统一结果
（``SinkResult``），具体落到哪由 ``config.memory.sink`` 决定：

- ``booku`` —— 框架自带 ``booku_memory:service:booku_memory``，
  写进去能被 RAG 检索到（**这是默认，也是推荐**）；
- ``local`` —— 只写本插件自己的 json（booku_memory 不在时的兜底）；
- ``none`` —— 只总结、不落记忆。

两个 booku 侧的硬约束（实测，写之前必须知道）：

1. ``create_memory`` 要求 core / diffusion / opposing **三个标签都非空**，
   少一个直接抛错。所以这里一定会用配置里的兜底标签补齐。
2. 新写的记忆默认进**隐现层**（``status=active``）：7 天内被激活少于 2 次的
   会被 booku 自己丢掉。想让它必留，把 ``memory.status`` 配成 ``archived``
   ——代价是默认检索不搜归档层。这是取舍，不是 bug。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from src.app.plugin_system.api import service_api
from src.app.plugin_system.api.log_api import get_logger

from . import state as state_module
from .config import ContextArchiverConfig

logger = get_logger("context_archiver.sink")

#: booku_memory 的服务组件签名。
BOOKU_SIGNATURE = "booku_memory:service:booku_memory"

#: 合法的 memory_type（booku 侧字段）。
VALID_MEMORY_TYPES: frozenset[str] = frozenset(
    {"event", "knowledge", "person", "place", "procedure"}
)


@dataclass
class MemoryItem:
    """一条待写入的记忆。

    Attributes:
        title: 标题（短，便于列表展示）。
        content: 正文（事实陈述，别写成台词）。
        memory_type: booku 的记忆类型：event / knowledge / person / place / procedure。
        core_tags: 核心标签（必填，空则用配置兜底）。
        diffusion_tags: 扩散标签（必填）。
        opposing_tags: 对立标签（必填）。
        person_id: 关联人物 id（可选）。
        event_start_at: 事件区间起点。
        event_end_at: 事件区间终点。
    """

    title: str = ""
    content: str = ""
    memory_type: str = "event"
    core_tags: list[str] = field(default_factory=list)
    diffusion_tags: list[str] = field(default_factory=list)
    opposing_tags: list[str] = field(default_factory=list)
    person_id: str = ""
    event_start_at: float = 0.0
    event_end_at: float = 0.0
    #: **存算一体**：写入时就问清楚「什么情况下该想起这条记忆」。
    #: 这些词不是给向量检索用的，是给本地倒排表用的——
    #: 对话里只要出现其中一个，这条记忆就被推给主模型，**运行时零 LLM**。
    triggers: list[str] = field(default_factory=list)
    #: 重要度提示：high = 跟安全/健康/承诺/金钱有关，值得主动提一句。
    risk: str = "normal"
    #: 隐私密度分层的「外流版」：跨流召回时用这一份代替 content。
    #: 空串表示这条没有外流版，跨流时按 ``privacy.gist_missing`` 处理。
    gist: str = ""

    def normalized(self, config: ContextArchiverConfig) -> MemoryItem:
        """用配置里的兜底值补齐空字段。

        Args:
            config: 插件配置。

        Returns:
            补全后的新实例（不改自身）。
        """
        memory_cfg = config.memory
        memory_type = str(self.memory_type or "").strip().lower()
        if memory_type not in VALID_MEMORY_TYPES:
            memory_type = "event"

        def _fill(values: list[str] | None, fallback: list[str]) -> list[str]:
            cleaned = [str(v).strip() for v in (values or []) if str(v).strip()]
            if cleaned:
                return cleaned[:8]
            return [str(v).strip() for v in fallback if str(v).strip()][:8]

        return MemoryItem(
            title=(str(self.title).strip() or "对话归档")[:80],
            content=str(self.content).strip(),
            memory_type=memory_type,
            core_tags=_fill(self.core_tags, list(memory_cfg.core_tags)),
            diffusion_tags=_fill(self.diffusion_tags, list(memory_cfg.diffusion_tags)),
            opposing_tags=_fill(self.opposing_tags, list(memory_cfg.opposing_tags)),
            person_id=str(self.person_id or "").strip(),
            event_start_at=float(self.event_start_at or 0.0),
            event_end_at=float(self.event_end_at or 0.0),
            # 触发词去重、去空白、限长（太长会污染倒排表）
            triggers=list(
                dict.fromkeys(
                    str(t).strip()[:16] for t in (self.triggers or []) if str(t).strip()
                )
            )[:12],
            risk="high" if str(self.risk or "").strip().lower() == "high" else "normal",
            gist=str(self.gist or "").strip(),
        )


@dataclass
class SinkResult:
    """记忆写入结果。

    Attributes:
        ok: 是否写入成功（sink=none 时恒为 True）。
        sink: 实际使用的 sink 名。
        written: 成功写入的条数。
        memory_ids: booku 返回的 memory_id 列表（local 为空）。
        error: 失败原因摘要。
        fallback_used: 是否因为 booku 不可用而降级到本地。
    """

    ok: bool = False
    sink: str = ""
    written: int = 0
    memory_ids: list[str] = field(default_factory=list)
    error: str = ""
    fallback_used: bool = False


def _extract_memory_id(result: Any) -> str:
    """从 ``create_memory`` 的返回里挖出 memory_id。"""
    if not isinstance(result, dict):
        return ""
    items = result.get("items")
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                for key in ("memory_id", "id"):
                    value = item.get(key)
                    if value:
                        return str(value)
    for key in ("memory_id", "id"):
        value = result.get(key)
        if value:
            return str(value)
    return ""


async def _write_booku(
    items: list[MemoryItem],
    config: ContextArchiverConfig,
    *,
    stream_id: str = "",
    source_stream_type: str = "",
) -> SinkResult:
    """写进框架自带 booku_memory。"""
    try:
        service = service_api.get_service(BOOKU_SIGNATURE)
    except Exception as error:  # noqa: BLE001 - 服务查询失败按不可用处理
        return SinkResult(ok=False, sink="booku", error=f"get service: {error}")

    if service is None:
        return SinkResult(ok=False, sink="booku", error="booku_memory 服务不可用")

    created_ids: list[str] = []
    errors: list[str] = []
    #: 存算一体的「存」半：写进 booku 的同时，把带触发词的目录登记到本地。
    #: 之后每一轮对话都是拿这份目录做本地匹配，不碰 booku、不调 LLM。
    index_records: list[dict[str, Any]] = []
    folder_id = str(config.memory.folder_id or "").strip()
    bucket = str(config.memory.bucket or "memory").strip() or "memory"
    status = str(config.memory.status or "active").strip() or "active"

    for item in items:
        if not item.content:
            continue
        kwargs: dict[str, Any] = {
            "title": item.title,
            "content": item.content,
            "bucket": bucket,
            "core_tags": item.core_tags,
            "diffusion_tags": item.diffusion_tags,
            "opposing_tags": item.opposing_tags,
            "memory_type": item.memory_type,
            "status": status,
            "event_start_at": item.event_start_at,
            "event_end_at": item.event_end_at,
        }
        if folder_id:
            kwargs["folder_id"] = folder_id
        if item.person_id and config.memory.attach_person:
            kwargs["person_id"] = item.person_id

        try:
            result = await service.create_memory(**kwargs)
        except Exception as error:  # noqa: BLE001 - 单条失败不影响其它条目
            logger.warning(f"[context_archiver] 写入记忆失败（{item.title}）: {error}")
            errors.append(f"{item.title}: {error}")
            continue

        memory_id = _extract_memory_id(result)
        if memory_id:
            created_ids.append(memory_id)
            index_records.append(
                {
                    "id": memory_id,
                    "title": item.title,
                    "snippet": (item.content or "")[:120],
                    "triggers": list(item.triggers or []),
                    "risk": item.risk,
                    "person": item.person_id,
                    "stream": stream_id,
                    "at": time.time(),
                    # ── 隐私密度分层 ──────────────────────────────────────────
                    # gist：这条记忆的「外流版」正文；跨流召回时用它代替全文。
                    # source_type：来源流类型（private / group），召回侧按它判方向。
                    # 两者都由写入时一次算好，读侧只做本地查表，零额外模型调用。
                    "gist": item.gist,
                    "source_type": source_stream_type,
                }
            )
            # 写入后立刻把激活计数抬 1。
            #
            # 为什么必须做：booku 的检索排序是 ``last_activated_at.desc()``，
            # 而刚写进去的记忆 activation_count = 0 —— 它永远排不上队、永远召不回来，
            # 于是 act 永远是 0，7 天后还会被隐现层当"没用的"丢掉。这是个死锁。
            # 抬 1 之后：召回时再 +1 就到 2，正好够晋升阈值。
            if config.memory.touch_on_write:
                try:
                    await service.update_activated(memory_id)
                except Exception as error:  # noqa: BLE001 - 抬计数失败不影响写入本身
                    logger.debug(
                        f"[context_archiver] 抬激活计数失败（{memory_id[:8]}）: {error}"
                    )

    if created_ids:
        if index_records:
            await state_module.append_memory_index(index_records)
        return SinkResult(
            ok=True,
            sink="booku",
            written=len(created_ids),
            memory_ids=created_ids,
            error="；".join(errors[:3]),
        )
    return SinkResult(ok=False, sink="booku", error="；".join(errors[:3]) or "没有可写入的条目")


async def _write_local(
    items: list[MemoryItem],
    stream_id: str,
    *,
    reason: str = "",
    source_stream_type: str = "",
) -> SinkResult:
    """写进本插件自己的 json（booku 不可用时的兜底）。"""
    import time as _time

    entries: list[dict[str, Any]] = []
    index_records: list[dict[str, Any]] = []
    now = _time.time()
    for position, item in enumerate(items):
        if not item.content:
            continue
        entries.append(
            {
                "at": now,
                "stream_id": stream_id,
                "title": item.title,
                "content": item.content,
                "memory_type": item.memory_type,
                "core_tags": item.core_tags,
                "diffusion_tags": item.diffusion_tags,
                "opposing_tags": item.opposing_tags,
                "person_id": item.person_id,
                "event_start_at": item.event_start_at,
                "event_end_at": item.event_end_at,
                "reason": reason,
                # 隐私密度分层：外流版与来源类型一并落盘（本地兜底也要能跨流/脱敏）。
                "gist": item.gist,
                "source_type": source_stream_type,
            }
        )
        # 本地兜底也要登记进触发词索引（存算一体的「存」半），否则 booku 一挂，
        # 写入还在、召回却空了。本地条目没有 booku 的 memory_id，所以自己造一个；
        # 又因为没有 read_full_content 可补，片段留长一点（500 字符）。
        index_records.append(
            {
                "id": f"local:{int(now * 1000)}-{position}",
                "title": item.title,
                "snippet": (item.content or "")[:500],
                "triggers": list(item.triggers or []),
                "risk": item.risk,
                "person": item.person_id,
                "stream": stream_id,
                "at": now,
                "gist": item.gist,
                "source_type": source_stream_type,
            }
        )

    if not entries:
        return SinkResult(ok=False, sink="local", error="没有可写入的条目")

    written = await state_module.append_local_memories(entries)
    if written and index_records:
        await state_module.append_memory_index(index_records)
    return SinkResult(
        ok=written,
        sink="local",
        written=len(entries) if written else 0,
        error="" if written else "本地 json 写入失败",
    )


async def write_memories(
    items: list[MemoryItem],
    config: ContextArchiverConfig,
    *,
    stream_id: str = "",
    reason: str = "",
    source_stream_type: str = "",
) -> SinkResult:
    """按配置把记忆写出去。

    ``sink=booku`` 且 booku 不可用时，会自动降级到本地 json（绝不静默丢内容），
    并在结果里用 ``fallback_used`` 标出来。

    Args:
        items: 待写入的记忆条目。
        config: 插件配置。
        stream_id: 来源聊天流（写日志与本地兜底用）。
        reason: 触发原因（写本地兜底记录用）。
        source_stream_type: 来源流类型（private / group / discuss），
            写进本地记忆索引，供召回侧的方向矩阵判定使用。

    Returns:
        写入结果。
    """
    sink_name = str(config.memory.sink or "booku").strip().lower()
    normalized = [item.normalized(config) for item in items]

    if sink_name == "none":
        return SinkResult(ok=True, sink="none", written=0)
    if not normalized:
        return SinkResult(ok=True, sink=sink_name, written=0)

    if sink_name == "local":
        return await _write_local(
            normalized,
            stream_id,
            reason=reason,
            source_stream_type=source_stream_type,
        )

    result = await _write_booku(
        normalized,
        config,
        stream_id=stream_id,
        source_stream_type=source_stream_type,
    )
    if result.ok:
        return result

    logger.warning(
        f"[context_archiver] booku 写入失败，降级到本地 json（stream={stream_id[:8]}）: {result.error}"
    )
    fallback = await _write_local(
        normalized,
        stream_id,
        reason=f"booku 降级: {result.error}",
        source_stream_type=source_stream_type,
    )
    fallback.fallback_used = True
    return fallback


__all__ = [
    "BOOKU_SIGNATURE",
    "VALID_MEMORY_TYPES",
    "MemoryItem",
    "SinkResult",
    "write_memories",
]
