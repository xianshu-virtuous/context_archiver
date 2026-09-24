"""context_archiver 归档执行器。

一条归档链路（顺序是刻意的，不能调换）：

1. **判定**：这个话题结束了吗（结束信号 / 闲置兜底 / 全局静默交叉验证）；
2. **取消息**：只取水位线之后的消息——水位线保证不重复总结；
3. **总结**：上次摘要 + 本次对话 → 新摘要 + 若干条记忆条目；
4. **落记忆**：写进 booku_memory（失败降级到本地 json，绝不静默丢）；
5. **清空**：**只在第 4 步成功后**才清空上下文（清空写的是持久水位线，
   不可逆；顺序错了就是永久丢一段经历）；
6. **审计**：无论成败都写一条，回答「那天到底归档了什么」。

``mode=observer`` 时整条链路只走到第 1 步：判定结果写日志，其余不动。
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.app.plugin_system.api import service_api, stream_api
from src.app.plugin_system.api.log_api import get_logger

from . import llm as llm_module
from . import state as state_module
from .config import ContextArchiverConfig
from .sink import MemoryItem, write_memories

logger = get_logger("context_archiver.archiver")

#: time_sense 的服务组件签名（**软依赖**：取不到就降级，不进 manifest 依赖）。
TIME_SENSE_SIGNATURE = "time_sense:service:time_sense"

#: 触发原因文案。
TRIGGER_END_SIGNAL = "end_signal"
TRIGGER_IDLE = "idle"
TRIGGER_MANUAL = "manual"

_SUMMARY_SYSTEM = """你是一个对话归档员。你的工作是把一段聊天记录压缩成两样东西：一份可以长期留存的摘要，以及若干条独立的记忆条目。

硬性要求：
1. 只输出一个 JSON 对象，不要任何解释、不要代码围栏之外的文字。
2. 摘要用第一人称、陈述语气，写「发生了什么、答应了什么、对方的状态和偏好有什么变化、有什么没说完的事」。不要写成可以被整句搬走的台词，不要写成对用户的汇报。
3. 记忆条目要**独立可检索**：单看一条也要能明白是谁、什么事、什么时候。每条 100 字以内。
4. 不要复述原话，不要编造对话里没有的信息；不确定的就不要写。
5. 标签三元组必须都给：core_tags（核心，1-3 个）、diffusion_tags（扩散，2-4 个）、opposing_tags（对立/反义，1-3 个）。
6. memory_type 只能取：event（发生了什么）/ person（关于人的事实）/ knowledge（学到的知识）/ place（地点）/ procedure（做法、流程）。

输出格式：
{
  "summary": "滚动摘要正文",
  "topic_ended": true,
  "memories": [
    {
      "title": "短标题",
      "content": "事实陈述",
      "memory_type": "event",
      "core_tags": ["标签1"],
      "diffusion_tags": ["标签2", "标签3"],
      "opposing_tags": ["标签4"]
    }
  ]
}

如果没有值得长期记住的内容，memories 就返回空数组，但 summary 必须写。"""

_SUMMARY_USER = """【归档时间】{now}
【聊天流】{stream_name}（{stream_id}）
【消息区间】{start_text} ~ {end_text}，共 {count} 条
【上次摘要】
{previous_summary}

【本次对话】
{digest}

请按系统提示的格式输出 JSON。"""


@dataclass
class ArchiveDecision:
    """一次「该不该归档」的判定结果。

    Attributes:
        should: 是否应该归档。
        reason: 判定原因文案（写日志/审计用）。
        trigger: 触发类型（``end_signal`` / ``idle`` / 空）。
    """

    should: bool = False
    reason: str = ""
    trigger: str = ""


@dataclass
class StreamSnapshot:
    """一次归档尝试的完整结果（给命令与服务用）。

    Attributes:
        stream_id: 聊天流标识。
        ok: 是否成功。
        trigger: 触发类型。
        message_count: 参与总结的消息条数。
        start_ts / end_ts: 消息区间。
        summary: 新摘要。
        memory_ids: 写入的记忆 id。
        sink: 实际 sink。
        cleared: 是否清空了上下文。
        fallback_used: 是否降级到本地 sink。
        error: 失败原因。
        dry_run: 是否演练（只总结不落盘、不清空）。
    """

    stream_id: str = ""
    ok: bool = False
    trigger: str = ""
    message_count: int = 0
    start_ts: float = 0.0
    end_ts: float = 0.0
    summary: str = ""
    memory_ids: list[str] = field(default_factory=list)
    sink: str = ""
    cleared: bool = False
    fallback_used: bool = False
    error: str = ""
    dry_run: bool = False

    def to_dict(self) -> dict[str, Any]:
        """转成可 JSON 化的字典。"""
        return {
            "stream_id": self.stream_id,
            "ok": self.ok,
            "trigger": self.trigger,
            "message_count": self.message_count,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "summary": self.summary,
            "memory_ids": list(self.memory_ids),
            "sink": self.sink,
            "cleared": self.cleared,
            "fallback_used": self.fallback_used,
            "error": self.error,
            "dry_run": self.dry_run,
        }


# --------------------------------------------------------------------------- #
# 消息读取与文本装配
# --------------------------------------------------------------------------- #


def _message_time(message: Any) -> float:
    """尽力取出消息时间戳（epoch 秒）。"""
    for attr in ("time", "timestamp", "created_at"):
        value = getattr(message, attr, None)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _message_person(message: Any) -> str:
    """尽力取出说话人 id。"""
    for attr in ("person_id", "user_id", "sender_id"):
        value = getattr(message, attr, None)
        if value:
            return str(value)
    return ""


def _message_text(message: Any) -> str:
    """尽力取出消息正文。"""
    for attr in ("processed_plain_text", "plain_text", "content", "text"):
        value = getattr(message, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    content = getattr(message, "content", None)
    if isinstance(content, list):
        parts = [
            str(getattr(part, "text", "") or "")
            for part in content
            if getattr(part, "text", None)
        ]
        joined = "".join(part for part in parts if part).strip()
        if joined:
            return joined
    return ""


def _clock(ts: float) -> str:
    """epoch 秒 → ``MM-DD HH:MM``。"""
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%m-%d %H:%M")
    except (OverflowError, OSError, ValueError):
        return "--"


async def collect_messages(
    stream_id: str,
    *,
    waterline_ts: float,
    max_messages: int,
) -> list[Any]:
    """取水位线之后的消息（按时间升序）。

    Args:
        stream_id: 聊天流标识。
        waterline_ts: 已归档水位线；只取比它新的消息。
        max_messages: 最多取多少条（取最近的那一批）。

    Returns:
        消息列表；读取失败返回空列表。
    """
    limit = max(10, min(int(max_messages), 5000))
    try:
        raw = await stream_api.get_stream_messages(stream_id, limit=limit, offset=0)
    except Exception as error:  # noqa: BLE001 - 读不到消息就当没有可归档内容
        logger.warning(f"[context_archiver] 读取流消息失败（{stream_id[:8]}）: {error}")
        return []

    messages = [m for m in (raw or []) if m is not None]
    messages.sort(key=_message_time)
    floor = float(waterline_ts or 0.0)
    if floor > 0:
        messages = [m for m in messages if _message_time(m) > floor]
    return messages


def build_digest(messages: list[Any], *, max_chars: int) -> str:
    """把消息列表拼成交给模型的对话文本（超出上限时从尾部截断）。

    Args:
        messages: 按时间升序的消息列表。
        max_chars: 文本长度上限（字符）。

    Returns:
        对话文本。
    """
    lines: list[str] = []
    for message in messages:
        text = _message_text(message)
        if not text:
            continue
        person = _message_person(message)
        speaker = person[:6] if person else "?"
        lines.append(f"[{_clock(_message_time(message))}] {speaker}: {text}")

    if not lines:
        return ""

    joined = "\n".join(lines)
    limit = max(500, int(max_chars))
    if len(joined) <= limit:
        return joined
    # 超长时保留最近的：从尾部往前凑，再翻回正序。
    kept: list[str] = []
    total = 0
    for line in reversed(lines):
        if total + len(line) + 1 > limit:
            break
        kept.append(line)
        total += len(line) + 1
    kept.reverse()
    return "（更早的内容已省略）\n" + "\n".join(kept)


# --------------------------------------------------------------------------- #
# 判定
# --------------------------------------------------------------------------- #


def evaluate(
    stream_state: state_module.StreamState,
    config: ContextArchiverConfig,
    *,
    now: float | None = None,
) -> ArchiveDecision:
    """判定某个流现在该不该归档（只看本流自己的信号）。

    Args:
        stream_state: 该流状态。
        config: 插件配置。
        now: 参考时刻（默认当前时间）。

    Returns:
        判定结果。
    """
    current = float(now if now is not None else time.time())
    trigger_cfg = config.trigger

    if stream_state.pending_count < int(trigger_cfg.min_messages):
        return ArchiveDecision(
            should=False,
            reason=f"消息数不足（{stream_state.pending_count} < {trigger_cfg.min_messages}）",
        )

    signal_at = float(stream_state.end_signal_at or 0.0)
    settled_at = float(stream_state.last_settled_signal_at or 0.0)
    if signal_at > settled_at and signal_at > 0:
        waited = current - signal_at
        if waited >= int(trigger_cfg.settle_seconds):
            return ArchiveDecision(
                should=True,
                reason=(
                    f"收到结束信号 {stream_state.end_signal_name or 'stop_conversation'}"
                    f"（已过 {int(waited)}s）"
                ),
                trigger=TRIGGER_END_SIGNAL,
            )
        return ArchiveDecision(
            should=False,
            reason=f"结束信号等待落库中（{int(waited)}s < {trigger_cfg.settle_seconds}s）",
        )

    last_activity = float(stream_state.last_activity_at or 0.0)
    if last_activity > 0:
        idle = current - last_activity
        if idle >= int(trigger_cfg.idle_seconds):
            return ArchiveDecision(
                should=True,
                reason=f"静默 {int(idle)}s（阈值 {trigger_cfg.idle_seconds}s）",
                trigger=TRIGGER_IDLE,
            )
        return ArchiveDecision(
            should=False,
            reason=f"仍然活跃（静默 {int(idle)}s < {trigger_cfg.idle_seconds}s）",
        )

    return ArchiveDecision(should=False, reason="没有活跃记录")


async def global_quiet_ok(config: ContextArchiverConfig) -> tuple[bool, str]:
    """用 time_sense 交叉验证「全局是否也安静」。

    time_sense 的「距上次说话」是**全局**的：任意一个流有人说话，它就会被刷新。
    所以本流静默 + 全局活跃 = 别处正在聊天，此时归档容易误判（人家只是切了个窗口）。

    time_sense 不存在、或其未启用时，直接放行（降级）。

    Args:
        config: 插件配置。

    Returns:
        ``(是否放行, 原因文案)``。
    """
    trigger_cfg = config.trigger
    if not (trigger_cfg.use_time_sense and trigger_cfg.require_global_quiet):
        return True, "未启用全局静默校验"

    try:
        service = service_api.get_service(TIME_SENSE_SIGNATURE)
    except Exception as error:  # noqa: BLE001 - 服务不可用按降级处理
        return True, f"time_sense 不可用（{error}）"

    if service is None:
        return True, "没有 time_sense"

    try:
        since = await service.since_last_message()
    except Exception as error:  # noqa: BLE001
        return True, f"time_sense 查询失败（{error}）"

    if not isinstance(since, dict) or not since.get("has_record"):
        return True, "time_sense 没有消息记录"

    seconds = float(since.get("seconds") or 0.0)
    threshold = float(trigger_cfg.idle_seconds)
    if seconds >= threshold:
        return True, f"全局静默 {int(seconds)}s"
    return False, f"全局仍有活动（{int(seconds)}s 前有人说话）"


# --------------------------------------------------------------------------- #
# 归档
# --------------------------------------------------------------------------- #


async def _summarize(
    config: ContextArchiverConfig,
    *,
    stream_id: str,
    stream_name: str,
    digest: str,
    previous_summary: str,
    start_ts: float,
    end_ts: float,
    count: int,
):
    """调一次模型，产出「摘要 + 记忆条目」。"""
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M")
    user_prompt = _SUMMARY_USER.format(
        now=now_text,
        stream_name=stream_name or "-",
        stream_id=stream_id[:8],
        start_text=_clock(start_ts),
        end_text=_clock(end_ts),
        count=count,
        previous_summary=(previous_summary or "（还没有摘要，这是第一次归档）")[:2000],
        digest=digest,
    )
    return await llm_module.call(
        config,
        system_prompt=_SUMMARY_SYSTEM,
        user_prompt=user_prompt,
        request_name="context_archiver_summary",
    )


def _parse_summary_payload(
    payload: dict[str, Any] | None,
    *,
    fallback_summary: str,
) -> tuple[str, list[MemoryItem], bool]:
    """把模型返回的 JSON 解析成（摘要, 记忆条目, 话题是否已结束）。"""
    if not isinstance(payload, dict):
        return fallback_summary, [], True

    summary = str(payload.get("summary") or "").strip()
    if not summary:
        summary = fallback_summary

    raw_memories = payload.get("memories")
    items: list[MemoryItem] = []
    if isinstance(raw_memories, list):
        for entry in raw_memories:
            if not isinstance(entry, dict):
                continue
            content = str(entry.get("content") or "").strip()
            if not content:
                continue
            items.append(
                MemoryItem(
                    title=str(entry.get("title") or "").strip(),
                    content=content,
                    memory_type=str(entry.get("memory_type") or "event"),
                    core_tags=[str(x) for x in (entry.get("core_tags") or []) if str(x).strip()],
                    diffusion_tags=[
                        str(x) for x in (entry.get("diffusion_tags") or []) if str(x).strip()
                    ],
                    opposing_tags=[
                        str(x) for x in (entry.get("opposing_tags") or []) if str(x).strip()
                    ],
                )
            )

    ended = bool(payload.get("topic_ended", True))
    return summary, items, ended


async def archive_stream(
    plugin: Any,
    stream_id: str,
    *,
    trigger: str,
    config: ContextArchiverConfig,
    dry_run: bool = False,
    force: bool = False,
) -> StreamSnapshot:
    """对单个流执行一次归档。

    Args:
        plugin: 插件实例（审计与状态用）。
        stream_id: 聊天流标识。
        trigger: 触发类型（结束信号 / 闲置 / 手动）。
        config: 插件配置。
        dry_run: 只总结、不落记忆、不清空（命令里预演用）。
        force: 忽略 min_messages 限制（手动归档时用）。

    Returns:
        本次归档的快照。
    """
    snapshot = StreamSnapshot(stream_id=stream_id, trigger=trigger, dry_run=dry_run)
    stream_state = await state_module.ArchiveStateStore.get(stream_id, fresh=True)

    messages = await collect_messages(
        stream_id,
        waterline_ts=float(stream_state.waterline_ts or 0.0),
        max_messages=int(config.trigger.max_messages),
    )
    snapshot.message_count = len(messages)
    if not messages:
        snapshot.error = "水位线之后没有新消息"
        await _write_audit(config, snapshot, summary="")
        return snapshot
    if not force and len(messages) < int(config.trigger.min_messages):
        snapshot.error = f"消息数不足（{len(messages)} < {config.trigger.min_messages}）"
        await _write_audit(config, snapshot, summary="")
        return snapshot

    start_ts = _message_time(messages[0])
    end_ts = _message_time(messages[-1])
    snapshot.start_ts = start_ts
    snapshot.end_ts = end_ts

    digest = build_digest(messages, max_chars=int(config.archive.max_digest_chars))
    if not digest:
        snapshot.error = "消息正文为空，无法总结"
        await _write_audit(config, snapshot, summary="")
        return snapshot

    person_counter = Counter(
        _message_person(m) for m in messages if _message_person(m)
    )
    dominant_person = person_counter.most_common(1)[0][0] if person_counter else ""

    stream_name = ""
    try:
        info = await stream_api.get_stream_info(stream_id)
        if isinstance(info, dict):
            stream_name = str(info.get("stream_name") or info.get("name") or "")
    except Exception:  # noqa: BLE001 - 拿不到名字不影响归档
        stream_name = ""

    result = await _summarize(
        config,
        stream_id=stream_id,
        stream_name=stream_name,
        digest=digest,
        previous_summary=stream_state.summary,
        start_ts=start_ts,
        end_ts=end_ts,
        count=len(messages),
    )
    if not result.ok:
        snapshot.error = f"总结调用失败: {result.error}"
        await _write_audit(config, snapshot, summary="")
        return snapshot

    payload = llm_module.extract_json(result.text)
    summary, items, _ended = _parse_summary_payload(
        payload,
        fallback_summary=result.text[: int(config.archive.summary_max_chars)],
    )
    summary = summary[: int(config.archive.summary_max_chars)]
    snapshot.summary = summary

    for item in items:
        if not item.person_id:
            item.person_id = dominant_person
        if not item.event_start_at:
            item.event_start_at = start_ts
        if not item.event_end_at:
            item.event_end_at = end_ts

    if dry_run:
        snapshot.ok = True
        snapshot.error = ""
        return snapshot

    sink_result = await write_memories(
        items,
        config,
        stream_id=stream_id,
        reason=trigger,
    )
    snapshot.sink = sink_result.sink
    snapshot.memory_ids = list(sink_result.memory_ids)
    snapshot.fallback_used = sink_result.fallback_used

    # ── 清空：只在记忆落地成功之后 ──────────────────────────────────────────
    cleared = False
    if config.archive.clear_context_enabled:
        if sink_result.ok:
            try:
                cleared = bool(await stream_api.load_and_clear_context(stream_id))
            except Exception as error:  # noqa: BLE001 - 清空失败不影响已写入的记忆
                logger.warning(f"[context_archiver] 清空上下文失败（{stream_id[:8]}）: {error}")
                snapshot.error = f"清空失败: {error}"
        else:
            logger.warning(
                f"[context_archiver] 记忆未写入成功，跳过清空（{stream_id[:8]}）: {sink_result.error}"
            )
            snapshot.error = f"记忆未写入成功，已跳过清空: {sink_result.error}"

    snapshot.cleared = cleared
    snapshot.ok = bool(sink_result.ok)

    # ── 状态推进 ────────────────────────────────────────────────────────────
    stream_state.waterline_ts = float(end_ts)
    stream_state.summary = summary
    stream_state.summary_updated_at = time.time()
    stream_state.last_archive_at = time.time()
    stream_state.archive_count = int(stream_state.archive_count or 0) + 1
    stream_state.pending_count = 0
    stream_state.last_settled_signal_at = float(stream_state.end_signal_at or 0.0)

    states = await state_module.ArchiveStateStore.load_all()
    states[stream_id] = stream_state
    await state_module.ArchiveStateStore.save_all(states, force=True)

    await _write_audit(config, snapshot, summary=summary)

    logger.info(
        f"[context_archiver] 归档完成：stream={stream_id[:8]} trigger={trigger} "
        f"messages={snapshot.message_count} memories={sink_result.written} "
        f"cleared={cleared} sink={sink_result.sink}"
    )
    return snapshot


async def _write_audit(
    config: ContextArchiverConfig,
    snapshot: StreamSnapshot,
    *,
    summary: str,
) -> None:
    """写一条审计记录（成败都写）。"""
    record = state_module.ArchiveAuditRecord(
        at=time.time(),
        stream_id=snapshot.stream_id,
        trigger=snapshot.trigger,
        message_count=snapshot.message_count,
        start_ts=snapshot.start_ts,
        end_ts=snapshot.end_ts,
        summary=summary[: int(config.archive.summary_max_chars)],
        memory_ids=list(snapshot.memory_ids),
        sink=snapshot.sink,
        cleared=snapshot.cleared,
        ok=snapshot.ok,
        error=snapshot.error,
    )
    await state_module.append_audit(record, keep=int(config.audit.keep_records))


# --------------------------------------------------------------------------- #
# 巡检
# --------------------------------------------------------------------------- #


async def tick(config: ContextArchiverConfig, *, now: float | None = None) -> list[dict[str, Any]]:
    """一次巡检：遍历所有有状态的流，该归档的归档。

    Args:
        config: 插件配置。
        now: 参考时刻（默认当前时间）。

    Returns:
        本次巡检的动作摘要（列表），供日志与服务展示。
    """
    current = float(now if now is not None else time.time())
    states = await state_module.ArchiveStateStore.load_all(fresh=True)
    if not states:
        return []

    actions: list[dict[str, Any]] = []
    observer = str(config.plugin.mode or "observer").strip().lower() == "observer"

    for stream_id, stream_state in list(states.items()):
        if not stream_id:
            continue
        decision = evaluate(stream_state, config, now=current)

        if not decision.should:
            if observer and config.observer.log_skipped_short:
                if current - float(stream_state.last_observer_log_at or 0.0) >= int(
                    config.observer.log_every_seconds
                ):
                    stream_state.last_observer_log_at = current
                    logger.info(
                        f"[context_archiver][observer] stream={stream_id[:8]} "
                        f"不会归档：{decision.reason}"
                    )
            continue

        if observer:
            if current - float(stream_state.last_observer_log_at or 0.0) >= int(
                config.observer.log_every_seconds
            ):
                stream_state.last_observer_log_at = current
                logger.info(
                    f"[context_archiver][observer] stream={stream_id[:8]} "
                    f"**将归档**：{decision.reason}（observer 模式不执行动作）"
                )
                actions.append(
                    {
                        "stream_id": stream_id,
                        "action": "observed",
                        "reason": decision.reason,
                        "trigger": decision.trigger,
                        "pending": stream_state.pending_count,
                    }
                )
            continue

        quiet_ok, quiet_reason = await global_quiet_ok(config)
        if not quiet_ok:
            if config.plugin.debug_log:
                logger.info(
                    f"[context_archiver] stream={stream_id[:8]} 暂不归档：{quiet_reason}"
                )
            continue

        snapshot = await archive_stream(
            None,
            stream_id,
            trigger=decision.trigger,
            config=config,
        )
        actions.append(
            {
                "stream_id": stream_id,
                "action": "archived" if snapshot.ok else "failed",
                "reason": decision.reason,
                "trigger": decision.trigger,
                "messages": snapshot.message_count,
                "memories": len(snapshot.memory_ids),
                "cleared": snapshot.cleared,
                "error": snapshot.error,
            }
        )

    await state_module.ArchiveStateStore.save_all(states)
    return actions


__all__ = [
    "TIME_SENSE_SIGNATURE",
    "TRIGGER_END_SIGNAL",
    "TRIGGER_IDLE",
    "TRIGGER_MANUAL",
    "ArchiveDecision",
    "StreamSnapshot",
    "archive_stream",
    "build_digest",
    "collect_messages",
    "evaluate",
    "global_quiet_ok",
    "tick",
]
