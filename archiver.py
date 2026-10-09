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
#: 按轮数触发：累积够多就沉淀一次，**只写记忆、绝不清空**（话题可能还在继续）。
TRIGGER_TURNS = "turns"

#: 每流保留多少个已归档 message_id 用于去重。要 ≥ trigger.max_messages，
#: 否则一次取满上限后，更早那批的 id 会被挤出去、导致重复总结。
_ARCHIVED_ID_KEEP = 800

_SUMMARY_SYSTEM = """你是一个对话归档员。你的工作是把一段聊天记录压缩成两样东西：一份可以长期留存的摘要，以及若干条独立的记忆条目。

硬性要求：
1. 只输出一个 JSON 对象，不要任何解释、不要代码围栏之外的文字。
2. 摘要用第一人称、陈述语气，写「发生了什么、答应了什么、对方的状态和偏好有什么变化、有什么没说完的事」。不要写成可以被整句搬走的台词，不要写成对用户的汇报。
3. 记忆条目要**独立可检索**：单看一条也要能明白是谁、什么事、什么时候。每条 100 字以内。
4. 不要复述原话，不要编造对话里没有的信息；不确定的就不要写。
5. 标签三元组必须都给：core_tags（核心，1-3 个）、diffusion_tags（扩散，2-4 个）、opposing_tags（对立/反义，1-3 个）。
6. memory_type 只能取：event（发生了什么）/ person（关于人的事实）/ knowledge（学到的知识）/ place（地点）/ procedure（做法、流程）。
7. **写足细节，但别撑破 JSON**：摘要 400 字以内，记忆 **6~10 条**，每条不超过 150 字。
   该留的细节：谁说了什么、答应了什么、数字与时间、原话里的关键措辞——
   这些比"聊了聊近况"有用得多。信息密度要高，不要为了凑字数注水。
8. 宁可少写一条，也不要让 JSON 被截断——被截断的输出等于什么都没总结。
   （这一条比第 7 条更重要：写满但完整，胜过多写一条却被截断。）
9. **每条记忆必须给 `triggers`**：写明「什么情况下该想起这条」。3~8 个中文词或短语，
   要覆盖三样东西——**同义说法、相关场景、相关物品**。
   例如「水壶昨天坏了，有点漏电」这条，triggers 应含：`喝水 渴 烧水 用水 用电 危险 安全`。
   这样对方哪怕只说一句「有点渴」，这条也能被想起来——这正是你写 triggers 的目的。
   另外给 `risk`：`high` 表示这条跟**安全 / 健康 / 承诺 / 金钱**有关、值得主动提一句；其余用 `normal`。

输出格式：
{
  "summary": "滚动摘要正文",
  "topic_ended": true,
  "memories": [
    {
      "title": "短标题",
      "content": "事实陈述",
      "gist": "外流版（仅当本轮被要求提供时才有；否则省略此字段）",
      "memory_type": "event",
      "core_tags": ["标签1"],
      "diffusion_tags": ["标签2", "标签3"],
      "opposing_tags": ["标签4"],
      "triggers": ["喝水", "渴", "烧水", "用电", "危险"],
      "risk": "high"
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


#: 隐私密度分层：仅当来源流命中 ``[privacy].apply_to`` 时才追加到 user prompt。
#:
#: 目的：让**同一次**总结调用额外产出一份「外流版」——她换个场合也能想起这件事，
#: 但只想起个印象，想不起内容。关系保留、内容抹除，是这里唯一的取舍原则。
_PRIVACY_GIST_RULE = """
【隐私分层要求】本流是{stream_kind}，本轮归档必须为**每条记忆**额外产出 `gist`（外流版）。

`content` 照原要求写、细节照留——那一份是给本流自己用的完整版。
`gist` 是这条记忆在**别的聊天场合**被想起时用的版本，必须做到：
**读得出一段关系的走向，读不出任何具体内容。**

gist 必须保留：
- 与对方关系的变化与温度（更亲近了 / 有过一次不愉快 / 约定了一件事）
- 情绪基调、大致的主题方向、时间

gist 必须抹除：
- 具体人名、昵称、身份、账号 —— 一律用「对方」「那个人」这类中性指代
- 具体说过的话、具体经过、具体数字与地点
- 身体、亲密、健康、财务等细节
- 任何能让第三方反推出对象或事件的信息

写法：不超过对应 content 的 {ratio}，通常一到两句；陈述语气，不写台词。
宁可更抽象，也不要为了信息量留下可识别的细节。
"""


def should_tier(config: ContextArchiverConfig, chat_type: str) -> bool:
    """本流是否要做隐私密度分层。

    判定是纯配置的、确定性的——**不交给模型判断要不要脱敏**。

    拿不到 ``chat_type``（空串）时返回 ``False``：此时记忆不会带 gist，
    跨流召回会按 ``privacy.gist_missing`` 处理（默认 drop），
    所以「判不出来」的结果是**不注入**，而不是裸奔。

    Args:
        config: 插件配置。
        chat_type: 当前流的聊天类型（private / group / discuss）。

    Returns:
        是否需要产出外流版。
    """
    privacy = config.privacy
    if not privacy.enabled:
        return False
    normalized = str(chat_type or "").strip().lower()
    if not normalized:
        return False
    targets = {str(x).strip().lower() for x in (privacy.apply_to or [])}
    return normalized in targets


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
    truncated: bool = False
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
            "truncated": self.truncated,
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


def _message_id(message: Any) -> str:
    """取消息的唯一标识（去重以此为准，不用时间戳）。"""
    for attr in ("message_id", "id"):
        value = getattr(message, attr, None)
        if value:
            return str(value)
    return ""


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
    waterline_ts: float = 0.0,
    exclude_ids: set[str] | None = None,
    max_messages: int,
) -> tuple[list[Any], bool]:
    """取还没归档过的消息（按时间升序）。

    **去重以 ``message_id`` 为准**（``exclude_ids``），时间戳只当**安全下界**。

    为什么不用时间戳做水位线：实测 ``Message.time`` 与数据库里的 ``time`` 不是同一套
    时钟，用「本批最晚一条」推水位线会把它推到未来（实测超前 94 秒），之后每次巡检
    都认为「水位线之后没有新消息」——**新对话永远归档不了**。这个坑踩过一次。

    翻页时从最新往回找，遇到「已归档过的 id」就停；这样既不重复总结，
    也不会跳过更早的未归档消息。

    Args:
        stream_id: 聊天流标识。
        waterline_ts: 已归档的**最早**时间（安全下界）；比它还旧的一律不看。
        exclude_ids: 已归档的 message_id 集合。
        max_messages: 单次最多取多少条（超出时取最早的，并在水位线上留出重叠）。

    Returns:
        ``(消息列表按时间升序, 是否被截断)``。
    """
    cap = max(10, min(int(max_messages), 5000))
    blocked = exclude_ids or set()
    floor = float(waterline_ts or 0.0)

    try:
        # 只取一页「最新的 cap 条」，然后**过滤**掉已归档的 —— 不翻页找边界。
        #
        # 两个坑（都踩过）：
        # 1. 框架的 get_stream_messages 查询用 .order_by("-id") 但返回前 reversed()，
        #    所以**返回是升序**（最旧在前），不是最新在前。按"最新在前"写翻页逻辑，
        #    第一个元素就是最旧的，一遇到已归档的 id 就停 → 永远返回空。
        # 2. 已归档的 id 与未归档的新消息是**交错**的，不是连续的块，
        #    "遇到就停"这种边界判断根本不成立。
        raw = await stream_api.get_stream_messages(stream_id, limit=cap, offset=0)
    except Exception as error:  # noqa: BLE001 - 读不到消息就当没有可归档内容
        logger.warning(f"[context_archiver] 读取流消息失败（{stream_id[:8]}）: {error}")
        return [], False

    if not raw:
        return [], False

    collected: list[Any] = []
    for message in raw:
        message_id = _message_id(message)
        if message_id and message_id in blocked:
            continue
        stamp = float(_message_time(message))
        # 时间戳只当「安全下界」，而且只在它明显有效时才用——
        # 取不到时间（0）的消息宁可多看一眼，也不要被误排除。
        if floor > 0 and stamp > 0 and stamp <= floor:
            continue
        collected.append(message)

    collected.sort(key=_message_time)
    if len(collected) > cap:
        collected = collected[:cap]

    # 取满一页说明可能还有更早的没取到：上位据此走保守分支（不清空）。
    return collected, len(raw) >= cap


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


def _backoff_seconds(streak: int, config: ContextArchiverConfig) -> float:
    """算第 ``streak`` 次连续失败后该退避多久（秒）。

    指数增长：base × 2^(streak-1)，封顶 ``retry_backoff_max_seconds``。
    ``retry_backoff_seconds = 0`` 表示关闭退避（返回 0）。
    """
    base = float(getattr(config.archive, "retry_backoff_seconds", 0) or 0)
    if streak <= 0 or base <= 0:
        return 0.0
    cap = float(getattr(config.archive, "retry_backoff_max_seconds", 0) or 0)
    if cap < base:
        cap = base
    # 指数不能直接算到爆：先按次数钳一下，避免 2**400 这种
    exponent = min(int(streak) - 1, 20)
    return min(cap, base * (2**exponent))


async def _mark_failure(
    stream_state: state_module.StreamState,
    config: ContextArchiverConfig,
    *,
    stream_id: str,
    error: str,
) -> None:
    """记一次归档失败并落盘。

    为什么必须落盘：失败不推进水位线（消息不会丢），但下一个 tick 会**立刻**重试。
    实测某个流因此连续失败 406 次、每次都是一次真实的模型调用，一条记忆都没写出来。
    把「连续失败次数 + 失败时刻」存下来，``evaluate`` 才能据此退避。
    """
    stream_state.fail_streak = int(stream_state.fail_streak or 0) + 1
    stream_state.last_fail_at = time.time()

    warn_after = max(1, int(getattr(config.archive, "retry_warn_after", 3) or 3))
    streak = stream_state.fail_streak
    if streak >= warn_after and (streak == warn_after or streak % warn_after == 0):
        wait = _backoff_seconds(streak, config)
        logger.warning(
            f"[context_archiver] 归档连续失败 {streak} 次"
            f"（stream={stream_id[:8]}），退避 {int(wait)}s 后再试。"
            f"最后一次错误：{error}"
        )

    try:
        states = await state_module.ArchiveStateStore.load_all()
        states[stream_id] = stream_state
        await state_module.ArchiveStateStore.save_all(states, force=True)
    except Exception as exc:  # noqa: BLE001 - 记不下来只影响退避，不影响主流程
        logger.debug(f"[context_archiver] 记录失败状态时出错（{stream_id[:8]}）: {exc}")


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

    # 失败退避放在最前面：结束信号/静默都拦不住「每 15 秒重试一次」，
    # 只有退避能拦。它只影响重试节奏，不会让消息被跳过。
    streak = int(stream_state.fail_streak or 0)
    last_fail = float(stream_state.last_fail_at or 0.0)
    if streak > 0 and last_fail > 0:
        wait = _backoff_seconds(streak, config)
        waited = current - last_fail
        if wait > 0 and waited < wait:
            return ArchiveDecision(
                should=False,
                reason=(
                    f"上次归档失败（连续 {streak} 次），退避中："
                    f"还需 {int(wait - waited)}s（退避 {int(wait)}s）"
                ),
            )

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

    # 判定基准是「**Bot 已经多久没参与**」，不是「流多久没动静」。
    # 群聊一直有人说话时 last_activity_at 永远在刷新，按它判就永远不会归档；
    # 而 last_engagement_at 只由 Bot 自己发言刷新，它才反映「Bot 是不是在潜水」。
    engagement = float(stream_state.last_engagement_at or 0.0)
    fallback = float(stream_state.last_activity_at or 0.0)
    base_at = engagement or fallback

    if base_at > 0:
        idle = current - base_at
        if idle >= int(trigger_cfg.idle_seconds):
            basis = "Bot 上次发言" if engagement else "尚无发言记录，按流活跃算"
            return ArchiveDecision(
                should=True,
                reason=(
                    f"Bot 已 {int(idle)}s 没参与"
                    f"（阈值 {trigger_cfg.idle_seconds}s；{basis}）"
                ),
                trigger=TRIGGER_IDLE,
            )

    # 轮数触发（可选，默认关）：话题可能还在继续，但这段该沉淀了。
    # 它只写记忆、不清空上下文 —— 话题还活着，清空会切断对话。
    if trigger_cfg.turn_trigger_enabled and stream_state.pending_count >= int(
        trigger_cfg.turn_threshold
    ):
        return ArchiveDecision(
            should=True,
            reason=(
                f"已累积 {stream_state.pending_count} 条消息"
                f"（阈值 {trigger_cfg.turn_threshold}）"
            ),
            trigger=TRIGGER_TURNS,
        )

    if base_at > 0:
        idle = current - base_at
        return ArchiveDecision(
            should=False,
            reason=(
                f"Bot 刚参与过（{int(idle)}s 前，阈值 {trigger_cfg.idle_seconds}s，"
                f"累积 {stream_state.pending_count} 条）"
            ),
        )

    return ArchiveDecision(should=False, reason="没有活跃记录")


def _time_sense_capabilities(service: object) -> dict[str, Any] | None:
    """取 time_sense 的能力清单（用于区分 v1 / v2）。

    ``capabilities()`` 是 time_sense 2.0 才有的方法；拿不到就说明对面是老版本，
    调用方退回旧口径，绝不去猜它的方法签名。

    Args:
        service: time_sense 服务实例。

    Returns:
        能力清单字典；老版本或调用失败时返回 ``None``。
    """
    caps_fn = getattr(service, "capabilities", None)
    if not callable(caps_fn):
        return None
    try:
        caps = caps_fn()
    except Exception:  # noqa: BLE001 - 探测失败按老版本处理
        return None
    return caps if isinstance(caps, dict) else None


async def global_quiet_ok(
    config: ContextArchiverConfig,
    *,
    stream_id: str = "",
) -> tuple[bool, str]:
    """交叉验证「别处是不是也在聊」——决定这一轮该不该归档。

    **语义**：本流已经满足归档条件了，这里只用来排除「人家只是切了个窗口」——
    别的流正热闹时归档容易误判，把没结束的对话总结掉。

    **口径演进**：time_sense 1.x 只有**全局**的「距上次说话」——任意一个流有人
    说话它就被刷新，于是「别的流在聊」和「这个流在聊」分不开。time_sense 2.0 起
    提供按流时钟（``since_last_message(stream_id=...)``）与流清单
    (``streams_overview()``)，所以这里改成：

    1. 先看**本流**自己的静默（够安静才继续）；
    2. 再看**除本流以外**最近的活跃流有没有超过阈值——这才是「别处」的真实含义。

    老版本 time_sense（无 ``capabilities()``）自动退回原来的全局口径；
    time_sense 不存在、未启用或查询失败时一律放行（降级，绝不因外部插件挡住归档）。

    Args:
        config: 插件配置。
        stream_id: 当前正在判断的聊天流；给了才能做按流校验。

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

    seconds_threshold = float(trigger_cfg.idle_seconds)
    capabilities = _time_sense_capabilities(service)

    if capabilities is not None and stream_id:
        try:
            own = await service.since_last_message(stream_id=stream_id)
            overview = await service.streams_overview(limit=50)
        except TypeError:  # 对面其实是老版本（不支持这些参数）→ 退回全局口径
            own = None
            overview = None
        except Exception as error:  # noqa: BLE001 - 查询失败按降级处理
            return True, f"time_sense 按流查询失败（{error}）"

        if isinstance(own, dict) and isinstance(overview, list):
            own_seconds = float(own.get("seconds") or 0.0)
            if own.get("has_record") and own_seconds < seconds_threshold:
                return False, (
                    f"本流 {int(own_seconds)}s 前还有人说话"
                    f"（阈值 {int(seconds_threshold)}s）"
                )

            foreign: list[tuple[float, str]] = []
            for row in overview:
                if not isinstance(row, dict):
                    continue
                other_id = str(row.get("stream_id") or "")
                if not other_id or other_id == str(stream_id):
                    continue
                silence = float(row.get("silence_seconds") or 0.0)
                if silence < seconds_threshold:
                    foreign.append((silence, other_id))
            if foreign:
                silence, other_id = min(foreign)
                return False, (
                    f"别处仍在活动（{other_id[:8]} {int(silence)}s 前说话，"
                    f"阈值 {int(seconds_threshold)}s）"
                )
            return True, f"按流校验通过：本流与其它流都安静超过 {int(seconds_threshold)}s"

    try:
        since = await service.since_last_message()
    except Exception as error:  # noqa: BLE001
        return True, f"time_sense 查询失败（{error}）"

    if not isinstance(since, dict) or not since.get("has_record"):
        return True, "time_sense 没有消息记录"

    seconds = float(since.get("seconds") or 0.0)
    threshold = float(trigger_cfg.idle_seconds)
    scope = "全局（time_sense 老版本口径）" if capabilities is None else "全局"
    if seconds >= threshold:
        return True, f"{scope}静默 {int(seconds)}s"
    return False, f"{scope}仍有活动（{int(seconds)}s 前有人说话）"


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
    chat_type: str = "",
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
    # 隐私密度分层：命中 apply_to 的流，在**同一次调用**里额外要一份外流版。
    # 不加调用次数、只多几十个输出 token，所以分层几乎是白拿的。
    if should_tier(config, chat_type):
        user_prompt = user_prompt + _PRIVACY_GIST_RULE.format(
            stream_kind=str(chat_type or "").strip().lower() or "私聊",
            ratio=f"{float(config.privacy.gist_max_ratio):.0%}",
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
                    # 存算一体的产物：触发词由写入时的模型产出（那时它有上下文、有常识），
                    # 之后查询只做本地字符串匹配 —— 运行时一次 LLM 都不用调。
                    triggers=[
                        str(x) for x in (entry.get("triggers") or []) if str(x).strip()
                    ],
                    risk=str(entry.get("risk") or "normal"),
                    # 隐私密度分层的「外流版」：跨流召回时用这一份代替 content。
                    # 模型没给就是空串，读侧按 privacy.gist_missing 处理（默认 drop）。
                    gist=str(entry.get("gist") or "").strip(),
                )
            )

    ended = bool(payload.get("topic_ended", True))
    return summary, items, ended


async def archive_stream(
    stream_id: str,
    *,
    trigger: str,
    config: ContextArchiverConfig,
    dry_run: bool = False,
    force: bool = False,
) -> StreamSnapshot:
    """对单个流执行一次归档。

    Args:
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

    messages, truncated = await collect_messages(
        stream_id,
        waterline_ts=float(stream_state.waterline_ts or 0.0),
        exclude_ids=set(stream_state.archived_ids or []),
        max_messages=int(config.trigger.max_messages),
    )
    snapshot.truncated = truncated
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
        await _mark_failure(
            stream_state, config, stream_id=stream_id, error=snapshot.error
        )
        await _write_audit(config, snapshot, summary="")
        return snapshot

    person_counter = Counter(
        _message_person(m) for m in messages if _message_person(m)
    )
    dominant_person = person_counter.most_common(1)[0][0] if person_counter else ""

    stream_name = ""
    chat_type = ""
    try:
        info = await stream_api.get_stream_info(stream_id)
        if isinstance(info, dict):
            stream_name = str(info.get("stream_name") or info.get("name") or "")
            # 隐私密度分层靠它决定「这个流要不要产出外流版」——
            # 判定输入必须来自流本身，不能靠模型猜。
            chat_type = str(info.get("chat_type") or "").strip().lower()
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
        chat_type=chat_type,
    )
    if not result.ok:
        snapshot.error = f"总结调用失败: {result.error}"
        await _mark_failure(
            stream_state, config, stream_id=stream_id, error=snapshot.error
        )
        await _write_audit(config, snapshot, summary="")
        return snapshot

    payload = llm_module.extract_json(result.text)
    if payload is None:
        # 解析失败 = 这次总结没有产出任何可用的东西。
        # **绝不能当成功**：那会推进水位线，让这段对话再也不会被总结 —— 永久跳过。
        # 把原文前 400 字符打进日志：不给原文就查不出「模型到底吐了什么」。
        preview = result.text[:400].replace("\n", " ⏎ ").replace("\r", "")
        snapshot.error = "模型输出不是合法 JSON"
        logger.warning(
            f"[context_archiver] 总结输出解析失败，水位线不推进"
            f"（stream={stream_id[:8]}，输出 {len(result.text)} 字符）"
            f"原文预览：{preview}"
        )
        await _mark_failure(
            stream_state, config, stream_id=stream_id, error=snapshot.error
        )
        await _write_audit(config, snapshot, summary="")
        return snapshot

    summary, items, _ended = _parse_summary_payload(payload, fallback_summary="")
    summary = summary[: int(config.archive.summary_max_chars)]
    # 外流版限长：模型偶尔会写长，超了就按 gist_max_ratio 截断。
    # 截断比放任好——跨流注入的每一句都该是「印象」，不是「细节」。
    if should_tier(config, chat_type):
        _ratio = float(config.privacy.gist_max_ratio)
        for item in items:
            if not (item.gist and item.content):
                continue
            _limit = max(20, int(len(item.content) * _ratio))
            if len(item.gist) > _limit:
                item.gist = item.gist[:_limit].rstrip() + "…"
    if not summary:
        # 模型没给摘要就保留上一次的——别把之前写好的摘要冲成空串。
        summary = str(stream_state.summary or "")
    snapshot.summary = summary

    for item in items:
        if not item.person_id:
            item.person_id = dominant_person
        if not item.event_start_at:
            item.event_start_at = start_ts
        if not item.event_end_at:
            item.event_end_at = end_ts
        # 给每条记忆附「原文坐标」：以后想抠细节，照这个时间区间回查消息原文。
        # 几乎不占 token（每条约 20 字符），却把「记得发生过」和「能查回细节」接上了——
        # 这是在不加总结成本的前提下提高记忆深度的关键一步。
        stamp = f"（{_clock(item.event_start_at)}）"
        if item.content and stamp not in item.content:
            item.content = item.content.rstrip() + stamp

    if dry_run:
        snapshot.ok = True
        snapshot.error = ""
        return snapshot

    sink_result = await write_memories(
        items,
        config,
        stream_id=stream_id,
        reason=trigger,
        source_stream_type=chat_type,
    )
    snapshot.sink = sink_result.sink
    snapshot.memory_ids = list(sink_result.memory_ids)
    snapshot.fallback_used = sink_result.fallback_used

    # ── 清空：只在记忆落地成功、且没有截断风险时 ────────────────────────────
    # 轮数触发（话题还活着）一律不清空——那是"沉淀记忆"，不是"结束话题"。
    cleared = False
    if config.archive.clear_context_enabled and trigger == TRIGGER_TURNS:
        if config.plugin.debug_log:
            logger.info(
                f"[context_archiver] 轮数触发只写记忆，不清空上下文（{stream_id[:8]}）"
            )
    elif config.archive.clear_context_enabled and truncated:
        logger.warning(
            f"[context_archiver] 消息数超过单次上限（{snapshot.message_count} 条），"
            f"为避免推进水位线跳过更早的未归档消息，本次保守跳过清空（{stream_id[:8]}）"
        )
        snapshot.error = "消息被截断，已跳过清空（保守）"
    elif config.archive.clear_context_enabled:
        if sink_result.ok:
            try:
                # 注意：框架保证这个接口**始终返回 True**（底层 clear_stream_context
                # 也是），所以 `cleared` 表达的是「调用没抛异常」，不是「确实清掉了多少」。
                # 真正的效果有两层：清掉内存里的 history/unread，并把
                # ChatStreams.context_cleared_at 写成当前时间 —— 重启后加载消息只取该
                # 时间点之后的，所以清空是持久的（已核对框架实现，不是推测）。
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
    # 去重靠 message_id 集合（Message.time 与数据库 time 不同源，时间戳不可信）；
    # 水位线只留「本批最早一条」当安全下界，宁可下次重叠也不超前。
    batch_ids = [_message_id(m) for m in messages if _message_id(m)]
    merged_ids = list(dict.fromkeys(list(stream_state.archived_ids or []) + batch_ids))
    stream_state.archived_ids = merged_ids[-_ARCHIVED_ID_KEEP:]
    stream_state.waterline_ts = float(start_ts)
    stream_state.summary = summary
    stream_state.summary_updated_at = time.time()
    stream_state.last_archive_at = time.time()
    stream_state.archive_count = int(stream_state.archive_count or 0) + 1
    stream_state.pending_count = 0
    stream_state.last_settled_signal_at = float(stream_state.end_signal_at or 0.0)
    # 成功就清空失败计数：退避只在「一直失败」时起作用，别让历史失败拖累正常节奏。
    stream_state.fail_streak = 0
    stream_state.last_fail_at = 0.0

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

        # 轮数触发是「聊着也要沉淀」，**不受全局静默限制**——
        # 否则 require_global_quiet 会永远把它挡死（聊天时全局从来不静默，
        # 实测 pending 涨到 169 却一次都没归档，就是因为这个）。
        if decision.trigger != TRIGGER_TURNS:
            quiet_ok, quiet_reason = await global_quiet_ok(config, stream_id=stream_id)
            if not quiet_ok:
                if config.plugin.debug_log:
                    logger.info(
                        f"[context_archiver] stream={stream_id[:8]} 暂不归档：{quiet_reason}"
                    )
                continue

        snapshot = await archive_stream(
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

    # 收尾：把「本 tick 自己改过的观测字段」合并进**盘上最新**的状态再落盘。
    #
    # **绝不能直接 save_all(states)**：这里的 states 是循环开始时 load_all(fresh=True)
    # 拿到的那份快照，而 archive_stream 内部又自己 fresh 取了一份**不同的对象**——
    # 归档成功后的水位线/archived_ids/summary、失败后的退避计数，全都写在那一份上。
    # 用陈旧快照回写会把它们全部抹掉（实测靠 save_all 的 3 秒节流侥幸没出事：
    # 归档成功后立刻回写会被节流拦住；一旦归档耗时超过 3 秒，就真的覆盖了
    # → 同一批消息被反复总结、反复写记忆，而且失败退避永远不生效）。
    latest = await state_module.ArchiveStateStore.load_all(fresh=True)
    for stream_id, state_in_tick in states.items():
        target = latest.get(stream_id)
        if target is None:
            continue
        # 本 tick 唯一会改的、且归档链路不碰的字段：observer 日志节流时间。
        target.last_observer_log_at = max(
            float(target.last_observer_log_at or 0.0),
            float(state_in_tick.last_observer_log_at or 0.0),
        )
    await state_module.ArchiveStateStore.save_all(latest)
    return actions


__all__ = [
    "TIME_SENSE_SIGNATURE",
    "TRIGGER_END_SIGNAL",
    "TRIGGER_IDLE",
    "TRIGGER_MANUAL",
    "TRIGGER_TURNS",
    "ArchiveDecision",
    "StreamSnapshot",
    "archive_stream",
    "build_digest",
    "collect_messages",
    "evaluate",
    "global_quiet_ok",
    "tick",
]
