"""context_archiver 状态层。

分三块：

- **每流状态**：活跃时间、结束信号、待归档水位线、滚动摘要——落盘在
  ``data/json_storage/context_archiver/``，命名空间 ``context_archiver``；
- **审计记录**：每次归档写一条（时间区间、消息条数、摘要、memory_id、是否清空），
  回答「那天到底归档了什么」；清空不可逆，这份记录是唯一的事后依据；
- **进程内缓存**：``service_api.get_service()`` 每次调用都新建实例，
  所以缓存与落盘节流必须挂在**类属性**上，放实例上等于没有。

读写一律「失败降级」：IO 出错返回默认值并记日志，绝不打断对话主流程。
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

from src.app.plugin_system.api import storage_api
from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("context_archiver.state")

#: JSON 存储命名空间。
STORE_NAME = "context_archiver"

#: 每流状态的存储键。
STREAMS_KEY = "streams"

#: 审计记录的存储键。
AUDIT_KEY = "audit"

#: 本地记忆兜底（sink=local）的存储键。
LOCAL_MEMORY_KEY = "local_memories"

#: 结束信号里被视为「话题结束」的 action 名。
END_ACTION_NAMES: frozenset[str] = frozenset({"stop_conversation"})

#: 结束信号里**不算**结束的 action 名（挂起等恢复，绝不能当结束）。
WAIT_ACTION_NAMES: frozenset[str] = frozenset({"pass_and_wait"})


# --------------------------------------------------------------------------- #
# 数据结构
# --------------------------------------------------------------------------- #


@dataclass
class StreamState:
    """单个聊天流的归档状态。

    Attributes:
        stream_id: 聊天流标识。
        last_activity_at: 最近一次活跃时刻（**流里任何一条消息**，含别人之间说的话）。
        last_activity_kind: 最近一次活跃的来源，排查用。
        last_engagement_at: 最近一次 **Bot 自己参与** 的时刻（它发了消息）。
            —— 这是写记忆的判定基准：群里一直有人说话时 ``last_activity_at``
            永远在刷新，只有 ``last_engagement_at`` 能反映「Bot 已经潜水多久」。
        last_engagement_kind: 参与来源。
        end_signal_at: 最近一次收到结束信号（stop_conversation）的时刻。
        end_signal_name: 结束信号的动作名。
        pending_count: 自上次归档以来累计的消息条数。
        waterline_ts: 已归档水位线的**安全下界**：用本批**最早**一条消息的时间，
            宁可下次重叠（靠 ``archived_ids`` 去重）也绝不超前——实测用「最晚一条」
            会把水位线推到比数据库还晚，之后所有巡检查都认为「没有新消息」。
        archived_ids: 最近已归档的 ``message_id``（精确定重）。时间戳不可靠，
            去重以这个为准。
        last_archive_at: 最近一次归档时刻。
        archive_count: 本流累计归档次数。
        summary: 滚动摘要——清空上下文后，靠它让 Bot 还记得这段聊过什么。
        summary_updated_at: 摘要最近更新时间。
        last_observer_log_at: observer 模式最近一次打日志的时间（防刷屏）。
        last_settled_signal_at: 已经归档过的那个结束信号时刻，避免重复触发。
    """

    stream_id: str = ""
    last_activity_at: float = 0.0
    last_activity_kind: str = ""
    last_engagement_at: float = 0.0
    last_engagement_kind: str = ""
    end_signal_at: float = 0.0
    end_signal_name: str = ""
    pending_count: int = 0
    waterline_ts: float = 0.0
    last_archive_at: float = 0.0
    archive_count: int = 0
    summary: str = ""
    summary_updated_at: float = 0.0
    last_observer_log_at: float = 0.0
    last_settled_signal_at: float = 0.0
    archived_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """转换为可落盘的字典。"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> StreamState:
        """从落盘字典恢复，字段缺失或类型异常时退回默认值。"""
        if not isinstance(data, dict):
            return cls()

        def _num(key: str) -> float:
            try:
                return float(data.get(key) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        def _int(key: str) -> int:
            try:
                return int(data.get(key) or 0)
            except (TypeError, ValueError):
                return 0

        return cls(
            stream_id=str(data.get("stream_id") or ""),
            last_activity_at=_num("last_activity_at"),
            last_activity_kind=str(data.get("last_activity_kind") or ""),
            last_engagement_at=_num("last_engagement_at"),
            last_engagement_kind=str(data.get("last_engagement_kind") or ""),
            end_signal_at=_num("end_signal_at"),
            end_signal_name=str(data.get("end_signal_name") or ""),
            pending_count=_int("pending_count"),
            waterline_ts=_num("waterline_ts"),
            last_archive_at=_num("last_archive_at"),
            archive_count=_int("archive_count"),
            summary=str(data.get("summary") or ""),
            summary_updated_at=_num("summary_updated_at"),
            last_observer_log_at=_num("last_observer_log_at"),
            last_settled_signal_at=_num("last_settled_signal_at"),
            archived_ids=[
                str(item)
                for item in (data.get("archived_ids") or [])
                if str(item).strip()
            ]
            if isinstance(data.get("archived_ids"), list)
            else [],
        )


@dataclass
class ArchiveAuditRecord:
    """一次归档尝试的审计记录（成功与失败都记）。

    Attributes:
        at: 记录时刻。
        stream_id: 聊天流标识。
        trigger: 触发原因（结束信号 / 闲置 / 手动）。
        message_count: 参与总结的消息条数。
        start_ts: 消息区间起点（epoch 秒）。
        end_ts: 消息区间终点（epoch 秒）。
        summary: 生成的摘要（截断后存，完整摘要另见 streams 状态）。
        memory_ids: 写入记忆后拿到的 id 列表。
        sink: 实际使用的 sink 名。
        cleared: 是否执行了清空。
        ok: 整体是否成功。
        error: 失败原因摘要。
    """

    at: float = 0.0
    stream_id: str = ""
    trigger: str = ""
    message_count: int = 0
    start_ts: float = 0.0
    end_ts: float = 0.0
    summary: str = ""
    memory_ids: list[str] = field(default_factory=list)
    sink: str = ""
    cleared: bool = False
    ok: bool = False
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        """转换为可落盘的字典。"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ArchiveAuditRecord:
        """从落盘字典恢复。"""
        if not isinstance(data, dict):
            return cls()

        def _num(key: str) -> float:
            try:
                return float(data.get(key) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        def _int(key: str) -> int:
            try:
                return int(data.get(key) or 0)
            except (TypeError, ValueError):
                return 0

        ids = data.get("memory_ids")
        return cls(
            at=_num("at"),
            stream_id=str(data.get("stream_id") or ""),
            trigger=str(data.get("trigger") or ""),
            message_count=_int("message_count"),
            start_ts=_num("start_ts"),
            end_ts=_num("end_ts"),
            summary=str(data.get("summary") or ""),
            memory_ids=[str(x) for x in ids] if isinstance(ids, list) else [],
            sink=str(data.get("sink") or ""),
            cleared=bool(data.get("cleared", False)),
            ok=bool(data.get("ok", False)),
            error=str(data.get("error") or ""),
        )


# --------------------------------------------------------------------------- #
# 存储：每流状态
# --------------------------------------------------------------------------- #


class ArchiveStateStore:
    """每流状态的进程内缓存 + 落盘。

    缓存与节流都是**类属性**：框架每次 ``get_service()`` 都会新建服务实例，
    实例属性带不走。
    """

    #: 进程内缓存：stream_id -> StreamState。
    _cache: dict[str, StreamState] | None = None

    #: 最近一次落盘的单调时钟读数，用于节流。
    _last_save_mono: float = -1.0e9

    #: 落盘最小间隔（秒）。
    _SAVE_THROTTLE: float = 3.0

    @classmethod
    async def load_all(cls, *, fresh: bool = False) -> dict[str, StreamState]:
        """读取全部流状态。

        Args:
            fresh: 是否强制从盘上重读（手动触发归档前用）。

        Returns:
            ``stream_id -> StreamState`` 的字典。
        """
        if not fresh and cls._cache is not None:
            return cls._cache

        try:
            raw = await storage_api.load_json(STORE_NAME, STREAMS_KEY)
        except Exception as error:  # noqa: BLE001 - 存储异常不应影响对话主流程
            logger.warning(f"[context_archiver] 读取流状态失败，按空状态处理: {error}")
            raw = None

        states: dict[str, StreamState] = {}
        if isinstance(raw, dict):
            for stream_id, payload in raw.items():
                state = StreamState.from_dict(payload if isinstance(payload, dict) else None)
                if not state.stream_id:
                    state.stream_id = str(stream_id)
                states[str(stream_id)] = state

        cls._cache = states
        return states

    @classmethod
    async def save_all(
        cls,
        states: dict[str, StreamState],
        *,
        force: bool = False,
    ) -> bool:
        """写入全部流状态。

        Args:
            states: 状态字典。
            force: 是否忽略落盘节流。

        Returns:
            是否真的写入成功。
        """
        current_mono = time.monotonic()
        if not force and (current_mono - cls._last_save_mono) < cls._SAVE_THROTTLE:
            return False

        payload = {sid: state.to_dict() for sid, state in states.items()}
        try:
            await storage_api.save_json(STORE_NAME, STREAMS_KEY, payload)
        except Exception as error:  # noqa: BLE001 - 存储异常不应影响对话主流程
            logger.warning(f"[context_archiver] 写入流状态失败: {error}")
            return False

        cls._cache = states
        cls._last_save_mono = current_mono
        return True

    @classmethod
    async def get(cls, stream_id: str, *, fresh: bool = False) -> StreamState:
        """取某个流的状态，不存在时新建（不落盘）。

        Args:
            stream_id: 聊天流标识。
            fresh: 是否强制从盘上重读。

        Returns:
            状态实例（永不为 ``None``）。
        """
        states = await cls.load_all(fresh=fresh)
        normalized = str(stream_id or "")
        state = states.get(normalized)
        if state is None:
            state = StreamState(stream_id=normalized)
            states[normalized] = state
        return state

    @classmethod
    def invalidate(cls) -> None:
        """丢弃进程内缓存（下次读取重新从盘上加载）。"""
        cls._cache = None
        cls._last_save_mono = -1.0e9


# --------------------------------------------------------------------------- #
# 存储：审计
# --------------------------------------------------------------------------- #


async def load_audit() -> list[ArchiveAuditRecord]:
    """读取审计记录（最近的在前）。

    Returns:
        审计记录列表；读取失败时返回空列表。
    """
    try:
        raw = await storage_api.load_json(STORE_NAME, AUDIT_KEY)
    except Exception as error:  # noqa: BLE001 - 审计读不到不该影响归档
        logger.warning(f"[context_archiver] 读取审计记录失败: {error}")
        return []
    if not isinstance(raw, list):
        return []
    records: list[ArchiveAuditRecord] = []
    for item in raw:
        records.append(ArchiveAuditRecord.from_dict(item if isinstance(item, dict) else None))
    return records


async def append_audit(record: ArchiveAuditRecord, *, keep: int) -> bool:
    """追加一条审计记录并裁剪到保留上限。

    Args:
        record: 审计记录。
        keep: 最多保留多少条。

    Returns:
        是否写入成功。
    """
    records = await load_audit()
    records.insert(0, record)
    limit = max(1, int(keep))
    if len(records) > limit:
        records = records[:limit]
    try:
        await storage_api.save_json(
            STORE_NAME,
            AUDIT_KEY,
            [item.to_dict() for item in records],
        )
        return True
    except Exception as error:  # noqa: BLE001 - 审计写不进去不阻断归档本身
        logger.warning(f"[context_archiver] 写入审计记录失败: {error}")
        return False


# --------------------------------------------------------------------------- #
# 存储：本地记忆兜底
# --------------------------------------------------------------------------- #


async def append_local_memories(entries: list[dict[str, Any]], *, keep: int = 2000) -> bool:
    """把记忆条目追加到本地 json（``sink=local`` 或 booku 不可用时兜底）。

    Args:
        entries: 记忆条目字典列表。
        keep: 最多保留多少条。

    Returns:
        是否写入成功。
    """
    if not entries:
        return True
    try:
        raw = await storage_api.load_json(STORE_NAME, LOCAL_MEMORY_KEY)
    except Exception:  # noqa: BLE001 - 读不到按空处理
        raw = None
    existing: list[Any] = list(raw) if isinstance(raw, list) else []
    existing.extend(entries)
    if len(existing) > keep:
        existing = existing[-keep:]
    try:
        await storage_api.save_json(STORE_NAME, LOCAL_MEMORY_KEY, existing)
        return True
    except Exception as error:  # noqa: BLE001
        logger.warning(f"[context_archiver] 写入本地记忆失败: {error}")
        return False


async def load_local_memories() -> list[dict[str, Any]]:
    """读取本地兜底记忆。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, LOCAL_MEMORY_KEY)
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


# --------------------------------------------------------------------------- #
# 存储：提示词归因审计
# --------------------------------------------------------------------------- #

#: 提示词归因审计的存储键。
PROMPT_AUDIT_KEY = "prompt_audit"

#: 真实输入构成统计的存储键（tools + payloads）。
INPUT_AUDIT_KEY = "input_audit"

#: 最近一次暴露的工具名清单。
TOOL_NAMES_KEY = "tool_names"

#: 每轮 payload 指纹（用来算「第一个变化出现在第几个 payload」= 缓存友好度）。
FINGERPRINT_KEY = "fingerprints"

#: 归因审计保留的最近样本条数。
_PROMPT_AUDIT_KEEP = 40

#: 需要累加的字段。
_AUDIT_FIELDS: tuple[str, ...] = ("history", "unreads", "extra", "extra_info", "other", "total")


class PromptAuditBuffer:
    """提示词归因的内存缓冲。

    ``on_prompt_build`` 在请求构建的同步路径上，每轮直接读改写 json 会给首字
    延迟添上几十毫秒。所以先攒在内存里，攒够条数或过了间隔再落盘一次——
    审计数据丢几个样本无所谓，拖慢对话不行。

    缓冲挂在**类属性**上：框架每次 ``get_service()`` 都新建服务实例，
    实例属性带不走。
    """

    #: 待落盘的样本。
    pending: list[dict[str, Any]] = []

    #: 待落盘的「真实输入构成」样本（BEFORE_LLM_REQUEST）。
    input_pending: list[dict[str, Any]] = []

    #: 最近一次落盘的单调时钟读数。
    last_flush_mono: float = -1.0e9

    #: 攒够多少条就落盘。
    flush_size: int = 8

    #: 最长多久落盘一次（秒）。
    flush_interval: float = 20.0


async def record_prompt_audit(record: dict[str, Any]) -> bool:
    """记录一条提示词构成样本（内存缓冲 + 定期落盘）。

    Args:
        record: 单轮样本，含 history / unreads / extra / other / total 等字符数。

    Returns:
        本次调用是否触发了落盘。
    """
    buffer = PromptAuditBuffer
    buffer.pending.append(record)

    now = time.monotonic()
    due = (
        len(buffer.pending) >= buffer.flush_size
        or (now - buffer.last_flush_mono) >= buffer.flush_interval
    )
    if not due:
        return False

    pending = list(buffer.pending)
    buffer.pending.clear()
    buffer.last_flush_mono = now
    return await _flush_prompt_audit(pending)


async def _flush_prompt_audit(samples: list[dict[str, Any]]) -> bool:
    """把缓冲区里的样本并进滚动统计并落盘。"""
    if not samples:
        return False

    try:
        raw = await storage_api.load_json(STORE_NAME, PROMPT_AUDIT_KEY)
    except Exception:  # noqa: BLE001 - 读不到按空处理
        raw = None
    data = raw if isinstance(raw, dict) else {}

    total_samples = int(data.get("samples") or 0)
    sums: dict[str, int] = {}
    raw_sums = data.get("sums")
    if isinstance(raw_sums, dict):
        for key in _AUDIT_FIELDS:
            try:
                sums[key] = int(raw_sums.get(key) or 0)
            except (TypeError, ValueError):
                sums[key] = 0
    else:
        sums = {key: 0 for key in _AUDIT_FIELDS}

    for item in samples:
        total_samples += 1
        for key in _AUDIT_FIELDS:
            try:
                sums[key] += int(item.get(key) or 0)
            except (TypeError, ValueError):
                continue

    recent = data.get("recent")
    merged: list[Any] = list(samples)
    if isinstance(recent, list):
        merged.extend(recent)
    merged = merged[:_PROMPT_AUDIT_KEEP]

    payload = {
        "samples": total_samples,
        "sums": sums,
        "first_at": float(data.get("first_at") or (samples[0].get("at") or 0.0)),
        "last_at": float(samples[-1].get("at") or 0.0),
        "recent": merged,
    }

    try:
        await storage_api.save_json(STORE_NAME, PROMPT_AUDIT_KEY, payload)
        return True
    except Exception as error:  # noqa: BLE001 - 审计写不进去不影响任何行为
        logger.warning(f"[context_archiver] 写入提示词归因失败: {error}")
        return False


async def flush_prompt_audit() -> bool:
    """强制把缓冲里的样本落盘（命令查询前调，保证看到最新数据）。"""
    pending = list(PromptAuditBuffer.pending)
    PromptAuditBuffer.pending.clear()
    PromptAuditBuffer.last_flush_mono = time.monotonic()
    return await _flush_prompt_audit(pending)


async def load_prompt_audit() -> dict[str, Any]:
    """读取提示词归因的汇总与最近样本。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, PROMPT_AUDIT_KEY)
    except Exception:  # noqa: BLE001
        return {}
    return raw if isinstance(raw, dict) else {}


# --------------------------------------------------------------------------- #
# 存储：召回与动作统计
# --------------------------------------------------------------------------- #

#: 召回统计的存储键。
RECALL_STATS_KEY = "recall_stats"

#: 动作调用统计的存储键。
ACTION_STATS_KEY = "action_stats"


class RuntimeStats:
    """召回与动作统计的内存增量。

    记录的是「自上次落盘以来的增量」，落盘时读盘上旧值累加——
    这样进程重启不会把历史清零，也不必每轮写盘。

    挂**类属性**：框架每次 ``get_service()`` 都新建实例。
    """

    #: 召回增量：rounds / injected / chars / src:<来源>
    recall_delta: dict[str, int] = {}

    #: 动作增量：<kind>:<name>
    action_delta: dict[str, int] = {}

    #: 最近一次落盘的单调时钟读数。
    last_flush_mono: float = -1.0e9

    #: 落盘间隔（秒）。
    flush_interval: float = 20.0


def _bump(bucket: dict[str, int], key: str, value: int = 1) -> None:
    """给计数器加一笔。"""
    bucket[key] = int(bucket.get(key, 0)) + int(value)


async def _merge_counters(key: str, delta: dict[str, int]) -> bool:
    """把增量并进盘上的计数器。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, key)
    except Exception:  # noqa: BLE001 - 读不到按空处理
        raw = None
    data = raw if isinstance(raw, dict) else {}
    counters = data.get("counters")
    merged: dict[str, int] = {}
    if isinstance(counters, dict):
        for name, value in counters.items():
            try:
                merged[str(name)] = int(value or 0)
            except (TypeError, ValueError):
                continue
    for name, value in delta.items():
        merged[name] = merged.get(name, 0) + int(value)
    try:
        await storage_api.save_json(
            STORE_NAME,
            key,
            {"counters": merged, "updated_at": time.time()},
        )
        return True
    except Exception as error:  # noqa: BLE001 - 统计写不进去不影响任何行为
        logger.warning(f"[context_archiver] 写入统计失败（{key}）: {error}")
        return False


async def flush_stats(*, force: bool = False) -> bool:
    """把统计增量落盘（命令查询前会 force 一次）。

    Args:
        force: 是否忽略落盘间隔。

    Returns:
        是否真的写了盘。
    """
    stats = RuntimeStats
    now = time.monotonic()
    if not force and (now - stats.last_flush_mono) < stats.flush_interval:
        return False

    recall_delta = dict(stats.recall_delta)
    action_delta = dict(stats.action_delta)
    if not recall_delta and not action_delta:
        stats.last_flush_mono = now
        return False

    stats.recall_delta.clear()
    stats.action_delta.clear()
    stats.last_flush_mono = now

    ok = True
    if recall_delta:
        ok = await _merge_counters(RECALL_STATS_KEY, recall_delta) and ok
    if action_delta:
        ok = await _merge_counters(ACTION_STATS_KEY, action_delta) and ok
    return ok


async def record_recall(injected: int, chars: int, sources: dict[str, int]) -> None:
    """记一笔召回统计（内存累加，按需落盘）。

    Args:
        injected: 本次注入了几条。
        chars: 注入文本字符数。
        sources: 各路贡献条数。
    """
    stats = RuntimeStats
    stats.recall_delta["rounds"] = int(stats.recall_delta.get("rounds", 0)) + 1
    _bump(stats.recall_delta, "injected", int(injected))
    _bump(stats.recall_delta, "chars", int(chars))
    for name, value in (sources or {}).items():
        _bump(stats.recall_delta, f"src:{name}", int(value or 0))
    await flush_stats()


async def record_action(
    *,
    name: str,
    kind: str,
    stream_id: str = "",
    success: bool = True,
    at: float = 0.0,
    scope: str = "",
) -> None:
    """记一笔动作/工具调用。

    Args:
        name: 动作或工具名。
        kind: ``action`` 或 ``tool``。
        stream_id: 所属聊天流（仅日志用）。
        success: 是否成功。
        at: 调用时刻。
        scope: 步进作用域名（只用于诊断，记成 ``scope:<值>``）。
    """
    if not name:
        return
    stats = RuntimeStats
    _bump(stats.action_delta, f"{kind}:{name}")
    if scope:
        # 不同的 chatter 用的 scope 名不一样，先记下来，别猜。
        _bump(stats.action_delta, f"scope:{scope}")
    if not success:
        _bump(stats.action_delta, f"{kind}:{name}:failed")
    await flush_stats()


async def load_recall_stats() -> dict[str, int]:
    """读取召回的累计统计。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, RECALL_STATS_KEY)
    except Exception:  # noqa: BLE001
        return {}
    if not isinstance(raw, dict):
        return {}
    counters = raw.get("counters")
    if not isinstance(counters, dict):
        return {}
    result: dict[str, int] = {}
    for name, value in counters.items():
        try:
            result[str(name)] = int(value or 0)
        except (TypeError, ValueError):
            continue
    return result


async def load_action_stats() -> dict[str, int]:
    """读取动作调用的累计统计。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, ACTION_STATS_KEY)
    except Exception:  # noqa: BLE001
        return {}
    if not isinstance(raw, dict):
        return {}
    counters = raw.get("counters")
    if not isinstance(counters, dict):
        return {}
    result: dict[str, int] = {}
    for name, value in counters.items():
        try:
            result[str(name)] = int(value or 0)
        except (TypeError, ValueError):
            continue
    return result


async def record_input_audit(record: dict[str, Any]) -> bool:
    """记一笔「真实输入构成」样本（内存缓冲 + 定期落盘）。

    Args:
        record: 单次样本，含 tools_chars / tools_count / payload_chars /
            total_chars / roles（各角色字符数）。

    Returns:
        本次调用是否触发了落盘。
    """
    buffer = PromptAuditBuffer
    buffer.input_pending.append(record)

    now = time.monotonic()
    due = (
        len(buffer.input_pending) >= buffer.flush_size
        or (now - buffer.last_flush_mono) >= buffer.flush_interval
    )
    if not due:
        return False

    pending = list(buffer.input_pending)
    buffer.input_pending.clear()
    buffer.last_flush_mono = now
    return await _flush_input_audit(pending)


async def _flush_input_audit(samples: list[dict[str, Any]]) -> bool:
    """把输入构成样本并进滚动统计并落盘。"""
    if not samples:
        return False

    try:
        raw = await storage_api.load_json(STORE_NAME, INPUT_AUDIT_KEY)
    except Exception:  # noqa: BLE001
        raw = None
    data = raw if isinstance(raw, dict) else {}

    total = int(data.get("samples") or 0)
    counters: dict[str, int] = {}
    raw_counters = data.get("counters")
    if isinstance(raw_counters, dict):
        for key, value in raw_counters.items():
            try:
                counters[str(key)] = int(value or 0)
            except (TypeError, ValueError):
                continue

    for item in samples:
        total += 1
        for key, value in item.items():
            if key in ("at", "request_name", "roles", "req"):
                continue
            try:
                counters[key] = counters.get(key, 0) + int(value)
            except (TypeError, ValueError):
                continue
        # 分组计数：roles → role:<角色>，req → req:<请求名>
        for group_key, prefix in (("roles", "role:"), ("req", "req:")):
            group = item.get(group_key)
            if isinstance(group, dict):
                for name, value in group.items():
                    try:
                        mark = f"{prefix}{name}"
                        counters[mark] = counters.get(mark, 0) + int(value)
                    except (TypeError, ValueError):
                        continue

    recent = data.get("recent")
    merged: list[Any] = list(samples[-10:])
    if isinstance(recent, list):
        merged.extend(recent[:10])

    payload = {
        "samples": total,
        "counters": counters,
        "first_at": float(data.get("first_at") or (samples[0].get("at") or 0.0)),
        "last_at": float(samples[-1].get("at") or 0.0),
        "recent": merged[:20],
    }
    try:
        await storage_api.save_json(STORE_NAME, INPUT_AUDIT_KEY, payload)
        return True
    except Exception as error:  # noqa: BLE001
        logger.warning(f"[context_archiver] 写入输入构成统计失败: {error}")
        return False


async def flush_input_audit() -> bool:
    """强制把输入构成缓冲落盘。"""
    pending = list(PromptAuditBuffer.input_pending)
    PromptAuditBuffer.input_pending.clear()
    PromptAuditBuffer.last_flush_mono = time.monotonic()
    return await _flush_input_audit(pending)


async def load_input_audit() -> dict[str, Any]:
    """读取输入构成统计。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, INPUT_AUDIT_KEY)
    except Exception:  # noqa: BLE001
        return {}
    return raw if isinstance(raw, dict) else {}


async def append_fingerprints(entry: dict[str, Any], *, keep: int = 40) -> bool:
    """追加一轮 payload 指纹（保留最近 keep 轮）。

    指纹 = 每个 payload 的「角色 / 字符数 / 内容哈希」序列。有了它就能算出
    **第一个内容变化出现在第几个 payload** —— 那正是缓存失效的起点：
    缓存按前缀匹配，从第一个不同处开始，后面全部作废。

    Args:
        entry: 单轮记录，含 at / fp（指纹数组）。
        keep: 保留多少轮。

    Returns:
        是否写入成功。
    """
    try:
        raw = await storage_api.load_json(STORE_NAME, FINGERPRINT_KEY)
    except Exception:  # noqa: BLE001
        raw = None
    data = raw if isinstance(raw, dict) else {}
    rounds = data.get("rounds")
    merged: list[Any] = list(rounds) if isinstance(rounds, list) else []
    merged.append(entry)
    if len(merged) > max(1, int(keep)):
        merged = merged[-max(1, int(keep)) :]

    try:
        await storage_api.save_json(
            STORE_NAME,
            FINGERPRINT_KEY,
            {"rounds": merged, "updated_at": time.time()},
        )
        return True
    except Exception as error:  # noqa: BLE001 - 记不下来不影响请求
        logger.warning(f"[context_archiver] 写入 payload 指纹失败: {error}")
        return False


async def load_fingerprints() -> list[dict[str, Any]]:
    """读取最近的 payload 指纹记录。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, FINGERPRINT_KEY)
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(raw, dict):
        return []
    rounds = raw.get("rounds")
    if not isinstance(rounds, list):
        return []
    return [item for item in rounds if isinstance(item, dict)]


async def save_tool_names(names: list[str]) -> bool:
    """记录当前暴露的工具名清单（覆盖式，只留最新一次）。

    要决定「砍哪些工具」，先得知道暴露了哪些。这个清单是那份决策的输入。

    Args:
        names: 工具名列表（已去重）。

    Returns:
        是否写入成功。
    """
    if not names:
        return False
    try:
        await storage_api.save_json(
            STORE_NAME,
            TOOL_NAMES_KEY,
            {"names": list(names), "count": len(names), "updated_at": time.time()},
        )
        return True
    except Exception as error:  # noqa: BLE001 - 记不下来不影响请求
        logger.warning(f"[context_archiver] 写入工具清单失败: {error}")
        return False


async def load_tool_names() -> list[str]:
    """读取最近一次暴露的工具名清单。"""
    try:
        raw = await storage_api.load_json(STORE_NAME, TOOL_NAMES_KEY)
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(raw, dict):
        return []
    names = raw.get("names")
    if not isinstance(names, list):
        return []
    return [str(item) for item in names if str(item).strip()]


__all__ = [
    "ACTION_STATS_KEY",
    "AUDIT_KEY",
    "END_ACTION_NAMES",
    "FINGERPRINT_KEY",
    "INPUT_AUDIT_KEY",
    "LOCAL_MEMORY_KEY",
    "PROMPT_AUDIT_KEY",
    "RECALL_STATS_KEY",
    "STORE_NAME",
    "STREAMS_KEY",
    "TOOL_NAMES_KEY",
    "WAIT_ACTION_NAMES",
    "ArchiveAuditRecord",
    "ArchiveStateStore",
    "PromptAuditBuffer",
    "RuntimeStats",
    "StreamState",
    "append_audit",
    "append_fingerprints",
    "append_local_memories",
    "flush_input_audit",
    "flush_prompt_audit",
    "flush_stats",
    "load_action_stats",
    "load_audit",
    "load_fingerprints",
    "load_input_audit",
    "load_local_memories",
    "load_prompt_audit",
    "load_recall_stats",
    "load_tool_names",
    "record_action",
    "record_input_audit",
    "record_prompt_audit",
    "record_recall",
    "save_tool_names",
]
