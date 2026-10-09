"""context_archiver 读取侧：自动召回。

**要解决的问题**：Bot 想记起一件事，现在得自己调记忆工具——那是一次额外的
agent 循环，一次 0.0350 元，而且模型经常想不起来调。这个模块把它变成系统行为：
在 prompt 构建时自动把该记得的捞出来塞进上下文，**Bot 一次 tool 都不用调**。

三条路线（``recall.mode``）：

- ``trigger``（存算一体的读半）：**写入时预计算，读取时纯本地匹配**。

  写记忆的那一刻，总结模型已经知道这条记忆「在什么场景下会被想起来」——
  于是让它顺手吐出 3~8 个触发词（同义词、场景、物件），落进本地索引
  （``storage:context_archiver/memory_index``）。之后每轮对话，插件把当前对话文本
  跟这张倒排表做**字符串包含匹配**：

  - 记忆：水壶昨天就坏了，有点漏电 → 触发词 ``["水壶", "喝水", "渴", "烧水", "漏电"]``
  - 用户说：好渴啊，有点想喝水 → 命中 ``喝水`` ``渴`` → **水壶那条被捞上来**

  这正是纯向量/纯关键词都做不到的关联：向量只认语义近邻、关键词只认字面，
  而「渴 → 水壶漏电」这条边是**写的时候就算好的**。查询侧零 LLM、零网络、零 tool，
  只是一张表加几个 ``in`` 判断，所以可以直接在 prompt 构建里同步跑。

- ``structured``（默认）：**绕过向量、绕过 tool**，全部走 booku 元数据层的 SQL——

  | 路子 | 用什么 | 特点 |
  | --- | --- | --- |
  | 人物路 | ``list_memory_entries(person_id=…)`` | 精确：关于当前对话者的事 |
  | 近因路 | 同接口按 ``last_activated_at`` 倒序 | 稳：最近被想起过的事 |
  | 关键词路 | ``grep_memories`` 字符级匹配 | 可选，中文分词噪音大，默认关 |

  零网络、毫秒级，所以可以直接在 prompt 构建里同步跑。

- ``embedding``：走 ``retrieve_memories``（EPA 向量检索），能命中语义相近的内容，
  代价是每轮一次 embedding 网络调用。

**闭环**：召回命中后调 ``update_activated`` 抬高激活计数——booku 的隐现层会在 7 天内
淘汰激活不足的记忆，而被召回过的东西本来就该留下。**读得越多，记忆越稳。**
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Any

from src.app.plugin_system.api import service_api
from src.app.plugin_system.api.log_api import get_logger

from . import state as state_module
from .config import ContextArchiverConfig

logger = get_logger("context_archiver.recall")

#: booku_memory 的服务组件签名。
BOOKU_SIGNATURE = "booku_memory:service:booku_memory"

#: 中文停用片段——出现这些的词不值得拿去 grep。
_STOP_TOKENS: frozenset[str] = frozenset(
    {
        "什么", "怎么", "可以", "这个", "那个", "现在", "今天", "明天", "昨天",
        "然后", "因为", "所以", "但是", "如果", "还是", "就是", "不是", "没有",
        "一下", "时候", "感觉", "我们", "你们", "他们", "自己", "知道", "觉得",
    }
)

#: 提取中文片段用的正则。
_CN_CHUNK = re.compile(r"[\u4e00-\u9fa5]{2,8}")


@dataclass
class RecallCandidate:
    """一条召回候选。

    Attributes:
    属性:
        memory_id: 记忆 id。
        title: 标题。
        content: 正文（可能是 snippet，取到全文后替换）。
        source: 来自哪条路（trigger / person / recent / keyword / embedding）。
        score: 排序分（越大越优先）。
        risk: 风险级别（``high`` 时会在注入文本里加醒目前缀）。
        source_type: 来源流类型（private / group / discuss）；空串 = 未知来源。
        source_stream: 来源流 id；判断「同不同流」用它。
        gist: 隐私密度分层的「外流版」正文；跨流注入时用它代替 ``content``。
    """

    memory_id: str = ""
    title: str = ""
    content: str = ""
    source: str = ""
    score: float = 0.0
    risk: str = "normal"
    source_type: str = ""
    source_stream: str = ""
    gist: str = ""


@dataclass
class RecallOutcome:
    """一次召回的结果。

    Attributes:
        block: 可直接注入 prompt 的文本（空串表示本次没有可注入内容）。
        injected_ids: 本次注入的记忆 id（写冷却用）。
        sources: 各路贡献条数。
        chars: 注入文本字符数。
        error: 失败原因（有值时 block 通常为空）。
    """

    block: str = ""
    injected_ids: list[str] = field(default_factory=list)
    sources: dict[str, int] = field(default_factory=dict)
    chars: int = 0
    error: str = ""


# --------------------------------------------------------------------------- #
# 文本提取
# --------------------------------------------------------------------------- #


def _length_of(value: Any) -> str:
    """把 values 里的某个字段安全转成字符串。"""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return "\n".join(str(v) for v in value if v)
    return ""


def extract_query_text(values: dict[str, Any], *, limit: int = 1200) -> str:
    """从 prompt 的 values 里取出「最近在聊什么」。

    优先用 ``unreads``（本轮新消息），不够再补 ``history`` 的尾部——
    检索意图应该由「刚说的话」决定，而不是整段历史。

    Args:
        values: ``on_prompt_build`` 的 values。
        limit: 取多少字符。

    Returns:
        查询文本；取不到时为空字符串。
    """
    parts: list[str] = []
    unreads = _length_of(values.get("unreads")).strip()
    if unreads:
        parts.append(unreads)

    remaining = max(0, limit - sum(len(p) for p in parts))
    if remaining > 0:
        history = _length_of(values.get("history")).strip()
        if history:
            parts.append(history[-remaining:])

    return "\n".join(parts)[:limit]


def extract_keywords(text: str, *, limit: int = 3) -> list[str]:
    """从文本里抠几个中文关键词（给 grep 路用）。

    中文没有空格分词，这里只取「连续 2~8 个汉字」的片段、去掉常见停用片段，
    按长度优先——够粗糙，所以关键词路默认关闭。

    Args:
        text: 源文本。
        limit: 最多返回几个。

    Returns:
        关键词列表。
    """
    if not text:
        return []
    chunks = [c for c in _CN_CHUNK.findall(text) if c not in _STOP_TOKENS]
    if not chunks:
        return []
    # 长的优先，其次去重保序
    ordered = sorted(set(chunks), key=lambda c: (-len(c), c))
    return ordered[: max(1, limit)]


def normalize_for_match(text: str) -> str:
    """把文本压成适合做「包含匹配」的形式（小写、去空白）。

    中文触发词里不会有空格，所以把查询侧的空白全去掉能提高命中率
    （``"有点 想喝水"`` → ``"有点想喝水"``）；标点保留——触发词里带标点的概率很低，
    而查询侧的标点不会挡住「触发词是查询子串」这个判断。
    """
    return re.sub(r"\s+", "", str(text or "").lower())


# --------------------------------------------------------------------------- #
# 检索
# --------------------------------------------------------------------------- #


async def _get_booku() -> Any:
    """取 booku_memory 服务实例（不可用时返回 ``None``）。"""
    try:
        service = service_api.get_service(BOOKU_SIGNATURE)
    except Exception as error:  # noqa: BLE001 - 服务不可用按无记忆处理
        logger.debug(f"[context_archiver] 取 booku_memory 服务失败: {error}")
        return None
    return service


def _items_of(payload: Any) -> list[dict[str, Any]]:
    """从各种返回结构里捞出条目列表。"""
    if not isinstance(payload, dict):
        return []
    for key in ("items", "results", "entries"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _candidate_from_item(item: dict[str, Any], *, source: str, score: float) -> RecallCandidate:
    """把工具返回项转成候选。"""
    return RecallCandidate(
        memory_id=str(item.get("id") or item.get("memory_id") or ""),
        title=str(item.get("title") or ""),
        content=str(item.get("content") or item.get("content_snippet") or ""),
        source=source,
        score=score,
    )


async def _recall_person(
    service: Any,
    config: ContextArchiverConfig,
    person_id: str,
) -> list[RecallCandidate]:
    """人物路：关于当前对话者的记忆。"""
    if not person_id:
        return []
    payload = await service.list_memory_entries(
        person_id=person_id,
        include_archived=bool(config.recall.include_archived),
        limit=max(1, int(config.recall.top_k) * 2),
    )
    return [
        _candidate_from_item(item, source="person", score=3.0 - index * 0.1)
        for index, item in enumerate(_items_of(payload))
    ]


async def _recall_recent(
    service: Any,
    config: ContextArchiverConfig,
) -> list[RecallCandidate]:
    """近因路：最近被激活过的记忆（不需要关键词，最稳）。"""
    limit = int(config.recall.recent_limit)
    if limit <= 0:
        return []
    payload = await service.list_memory_entries(
        include_archived=bool(config.recall.include_archived),
        limit=max(1, limit),
    )
    return [
        _candidate_from_item(item, source="recent", score=2.0 - index * 0.1)
        for index, item in enumerate(_items_of(payload))
    ]


async def _recall_keyword(
    service: Any,
    config: ContextArchiverConfig,
    query_text: str,
) -> list[RecallCandidate]:
    """关键词路：字符级匹配（默认关闭）。"""
    limit = int(config.recall.keyword_limit)
    if limit <= 0:
        return []
    candidates: list[RecallCandidate] = []
    for keyword in extract_keywords(query_text, limit=limit):
        try:
            payload = await service.grep_memories(
                query=keyword,
                search_fields=["title", "content", "tags"],
                include_archived=bool(config.recall.include_archived),
                top_k=limit,
            )
        except Exception as error:  # noqa: BLE001 - 单次 grep 失败不影响其它路
            logger.debug(f"[context_archiver] grep 失败（{keyword}）: {error}")
            continue
        for index, item in enumerate(_items_of(payload)):
            candidate = _candidate_from_item(item, source="keyword", score=2.5 - index * 0.1)
            candidates.append(candidate)
    return candidates


async def _recall_triggers(
    config: ContextArchiverConfig,
    query_text: str,
    *,
    person_id: str = "",
) -> list[RecallCandidate]:
    """触发词路（存算一体的读半）：把当前对话文本跟本地触发词表做包含匹配。

    **零网络、零 LLM、零 tool**——只读一份本地 JSON 索引。所以它就写在
    prompt 构建的同步路径上，几十微秒级。

    打分（越大越优先，后面会和其它路一起按分裁剪）：

    - 命中的触发词：多字命中每个 +1.5，单字命中每个 +0.5，底分 3.0
    - 佐证门槛：只有单字命中且不足两个 → 丢弃（一个「水」字不值得惊动一条记忆）
    - ``risk == "high"``（安全/健康/承诺/钱）：+ ``trigger_risk_bonus``
    - 人物一致（写这条记忆时记录的 person 就是现在这个人）：+ ``trigger_person_bonus``
    - 时间衰减：``0.5 ** (小时数 / 半衰期)``，默认半衰期 72 小时

    Args:
        config: 插件配置。
        query_text: 当前对话文本（unreads + history 尾部）。
        person_id: 当前对话者 id（群聊时为空）。

    Returns:
        候选列表（可能为空）。
    """
    trigger_cfg = config.recall
    limit = int(trigger_cfg.trigger_limit)
    if limit <= 0:
        return []

    query = normalize_for_match(query_text)
    if len(query) < 2:
        return []

    try:
        entries = await state_module.load_memory_index()
    except Exception as error:  # noqa: BLE001 - 索引读不到就当没命中
        logger.debug(f"[context_archiver] 读本地记忆索引失败: {error}")
        return []

    now = time.time()
    half_life_hours = max(1.0, float(trigger_cfg.trigger_half_life_hours))
    candidates: list[RecallCandidate] = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        memory_id = str(entry.get("id") or "")
        if not memory_id:
            continue

        hits_multi = 0
        hits_single = 0
        for trigger in entry.get("triggers") or []:
            word = normalize_for_match(trigger)
            if not word or word not in query:
                continue
            if len(word) >= 2:
                hits_multi += 1
            else:
                # 单字触发词（"渴"、"水"）很有用——「渴」正是把水壶那条牵出来的边。
                # 但它太泛（"水"能命中一半对话），所以只给低权重，并且要求佐证：
                # 光靠一个单字命中的记忆不值得被捞上来。
                hits_single += 1
        if not hits_multi and hits_single < 2:
            continue

        score = 3.0 + 1.5 * hits_multi + 0.5 * hits_single
        risk = str(entry.get("risk") or "normal").strip().lower()
        if risk == "high":
            score += float(trigger_cfg.trigger_risk_bonus)
        if person_id and str(entry.get("person") or "") == person_id:
            score += float(trigger_cfg.trigger_person_bonus)

        written_at = entry.get("at")
        if isinstance(written_at, (int, float)) and written_at > 0:
            age_hours = max(0.0, (now - float(written_at)) / 3600.0)
            score *= 0.5 ** (age_hours / half_life_hours)

        candidates.append(
            RecallCandidate(
                memory_id=memory_id,
                title=str(entry.get("title") or ""),
                content=str(entry.get("snippet") or ""),
                source="trigger",
                score=score,
                risk="high" if risk == "high" else "normal",
                # 触发词路本来就直读本地索引，来源与外部版这里现成，不用回查。
                source_type=str(entry.get("source_type") or "").strip().lower(),
                source_stream=str(entry.get("stream") or ""),
                gist=str(entry.get("gist") or ""),
            )
        )

    candidates.sort(key=lambda c: -c.score)
    return candidates[:limit]


async def _recall_embedding(
    service: Any,
    config: ContextArchiverConfig,
    query_text: str,
) -> list[RecallCandidate]:
    """向量路：走 retrieve_memories。"""
    if not query_text.strip():
        return []
    payload = await service.retrieve_memories(
        query_text=query_text,
        top_k=max(1, int(config.recall.embedding_top_k)),
        include_archived=bool(config.recall.include_archived),
    )
    return [
        _candidate_from_item(item, source="embedding", score=4.0 - index * 0.1)
        for index, item in enumerate(_items_of(payload))
    ]


async def _fetch_contents(service: Any, memory_ids: list[str]) -> dict[str, str]:
    """按 id 批量取完整正文（search/grep 返回的正文是截断的）。"""
    if not memory_ids:
        return {}
    try:
        payload = await service.read_full_content(memory_ids=list(memory_ids))
    except Exception as error:  # noqa: BLE001 - 取不到全文就退回 snippet
        logger.debug(f"[context_archiver] 取记忆正文失败: {error}")
        return {}

    contents: dict[str, str] = {}
    for item in _items_of(payload):
        memory_id = str(item.get("id") or item.get("memory_id") or "")
        content = str(item.get("content") or "").strip()
        if memory_id and content:
            contents[memory_id] = content
    return contents


async def _touch_activated(service: Any, memory_ids: list[str]) -> None:
    """抬高激活计数（防止这些记忆被隐现层的 7 天规则丢掉）。"""
    for memory_id in memory_ids:
        try:
            await service.update_activated(memory_id)
        except Exception as error:  # noqa: BLE001 - 计数失败不影响注入
            logger.debug(f"[context_archiver] 更新激活计数失败（{memory_id[:8]}）: {error}")


# --------------------------------------------------------------------------- #
# 组装
# --------------------------------------------------------------------------- #

_BLOCK_HEADER = """## 你想起了一些事
以下是你记忆里与当前对话相关的内容（自动回想，不是让你念出来的台词，别复述原句；
带 ⚠ 的是安全/健康/承诺/金钱相关的事，该提醒就主动提醒）："""


def build_block(candidates: list[RecallCandidate], *, max_chars: int) -> str:
    """把候选组装成注入文本（按分排序、按字符预算截断）。

    Args:
        candidates: 候选列表（已去重）。
        max_chars: 正文总字符上限。

    Returns:
        注入文本；无内容时为空串。
    """
    ordered = sorted(candidates, key=lambda c: -c.score)
    lines: list[str] = []
    used = len(_BLOCK_HEADER)

    for candidate in ordered:
        text = (candidate.content or "").strip()
        if not text:
            continue
        title = (candidate.title or "").strip()
        entry = f"- {title}：{text}" if title else f"- {text}"
        if candidate.risk == "high":
            # 安全/健康/承诺/钱这类事，值得让模型明确知道"这条要主动说"。
            entry = f"- ⚠ {title}：{text}" if title else f"- ⚠ {text}"
        if used + len(entry) > max_chars:
            remaining = max_chars - used
            if remaining < 40:
                break
            entry = entry[:remaining] + "…"
        lines.append(entry)
        used += len(entry)

    if not lines:
        return ""
    return _BLOCK_HEADER + "\n" + "\n".join(lines) + "\n"

# 供外部引用（避免 lint 说未使用）
_BLOCK_HEADER_TEXT = _BLOCK_HEADER


# --------------------------------------------------------------------------- #
# 隐私密度分层：来源回填 → 方向过滤 → 跨流换外流版
# --------------------------------------------------------------------------- #


async def attach_scope_info(candidates: list[RecallCandidate]) -> None:
    """用本地记忆索引，给每个候选补上「来源流类型 / 来源流 id / 外流版」。

    为什么必须查本地索引、而不是问 booku：
    booku 的记忆检索是**全局**的，条目里没有「这条是从哪个流写下来的」这个概念，
    而这件事只有插件自己知道——所以写入时记进 ``memory_index``，召回时按 id 查表回填。
    查表是纯本地操作，零网络、零模型。

    Args:
        candidates: 待回填的候选（原地修改）。
    """
    if not candidates:
        return
    try:
        entries = await state_module.load_memory_index()
    except Exception as error:  # noqa: BLE001 - 读不到就当没有来源信息
        logger.debug(f"[context_archiver] 读本地记忆索引失败（来源回填跳过）: {error}")
        return

    mapping: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if isinstance(entry, dict):
            entry_id = str(entry.get("id") or "")
            if entry_id:
                mapping[entry_id] = entry

    for candidate in candidates:
        entry = mapping.get(candidate.memory_id)
        if entry is None:
            continue
        candidate.source_type = str(entry.get("source_type") or "").strip().lower()
        candidate.source_stream = str(entry.get("stream") or "")
        candidate.gist = str(entry.get("gist") or "")


def is_cross_stream(
    candidate: RecallCandidate,
    *,
    current_stream: str,
    current_type: str,
) -> bool:
    """这条记忆是不是要「跨流」注入（即离开它自己的来源流）。

    拿不准就返回 ``False``——同流注入没有隐私问题，不该因为保守而误伤。

    Args:
        candidate: 候选。
        current_stream: 当前流 id。
        current_type: 当前流类型。

    Returns:
        是否需要按跨流处理。
    """
    source_stream = str(candidate.source_stream or "")
    if not source_stream:
        # 没有来源流信息（旧记忆）：只有当前流类型跟来源类型不一致时才算跨流。
        source_type = str(candidate.source_type or "").strip().lower()
        current = str(current_type or "").strip().lower()
        return bool(source_type and current and source_type != current)
    return source_stream != str(current_stream or "")


def scope_allows(
    candidate: RecallCandidate,
    *,
    current_stream: str,
    current_type: str,
    scope: Any,
) -> bool:
    """方向矩阵：这条记忆允不允许出现在当前流。

    纯函数——只看配置与来源，**不调模型、不做语义判断**。
    这正是它跟 ``cross_stream_relay`` 的 relay 机制的区别：
    那边是让模型自己决定「要不要过去说」，这里是配置决定。

    Args:
        candidate: 候选。
        current_stream: 当前流 id。
        current_type: 当前流类型。
        scope: ``config.recall_scope``。

    Returns:
        是否放行。
    """
    source = str(candidate.source_type or "").strip().lower()
    current = str(current_type or "").strip().lower()

    if not source:
        # 未知来源（1.2.0 之前写入的旧记忆）：默认 allow，不因升级而失效。
        return (
            str(getattr(scope, "unknown_source", "allow") or "allow").strip().lower()
            != "deny"
        )

    group_like = ("group", "discuss")
    same_stream = str(candidate.source_stream or "") == str(current_stream or "")

    if source == "private" and current == "private":
        # 私聊之间只认自己那一条——`private_to_group` 管不到这里，恒关。
        return same_stream
    if source == "private" and current in group_like:
        return bool(getattr(scope, "private_to_group", False))
    if source in group_like and current == "private":
        return bool(getattr(scope, "group_to_private", True))
    if source in group_like and current in group_like:
        return True if same_stream else bool(getattr(scope, "group_to_group", False))
    return True


async def recall_for_prompt(
    config: ContextArchiverConfig,
    values: dict[str, Any],
    *,
    exclude_ids: set[str] | None = None,
    person_id: str = "",
    stream_id: str = "",
    chat_type: str = "",
) -> RecallOutcome:
    """执行一次召回并组装注入文本。

    Args:
        config: 插件配置。
        values: ``on_prompt_build`` 的 values。
        exclude_ids: 需要排除的记忆 id（冷却中的）。
        person_id: 当前对话者 id（人物路用）。

    Returns:
        召回结果。
    """
    recall_cfg = config.recall
    if not recall_cfg.enabled:
        return RecallOutcome(error="未启用")

    # 当前流信息：优先用调用方传进来的，没有就退回 values 里的 stream_id。
    current_stream = str(stream_id or values.get("stream_id") or "").strip()
    current_type = str(chat_type or "").strip().lower()

    service = await _get_booku()
    if service is None:
        return RecallOutcome(error="booku_memory 不可用")

    mode = str(recall_cfg.mode or "structured").strip().lower()
    query_text = extract_query_text(values)
    sources: dict[str, int] = {}

    async def _run() -> list[RecallCandidate]:
        collected: list[RecallCandidate] = []
        # 触发词路（存算一体）：本地索引匹配，零网络，所以放在最前面也无所谓开销。
        if recall_cfg.trigger_enabled and mode in ("trigger", "structured", "both"):
            trigger_hits = await _recall_triggers(config, query_text, person_id=person_id)
            sources["trigger"] = len(trigger_hits)
            collected.extend(trigger_hits)
        if mode in ("structured", "both"):
            if recall_cfg.person_first and person_id:
                person_hits = await _recall_person(service, config, person_id)
                sources["person"] = len(person_hits)
                collected.extend(person_hits)
            recent_hits = await _recall_recent(service, config)
            sources["recent"] = len(recent_hits)
            collected.extend(recent_hits)
            if recall_cfg.keyword_enabled:
                keyword_hits = await _recall_keyword(service, config, query_text)
                sources["keyword"] = len(keyword_hits)
                collected.extend(keyword_hits)
        if mode in ("embedding", "both"):
            embedding_hits = await _recall_embedding(service, config, query_text)
            sources["embedding"] = len(embedding_hits)
            collected.extend(embedding_hits)
        return collected

    try:
        candidates = await asyncio.wait_for(
            _run(), timeout=max(0.2, float(recall_cfg.timeout_seconds))
        )
    except asyncio.TimeoutError:
        return RecallOutcome(error=f"召回超时（>{recall_cfg.timeout_seconds}s）", sources=sources)
    except Exception as error:  # noqa: BLE001 - 召回失败绝不影响对话
        return RecallOutcome(error=f"{type(error).__name__}: {error}", sources=sources)

    # ── 隐私密度分层：回填来源 → 按方向拦 → 跨流换外流版 ──────────────────
    # 拦在汇聚点而不是各路内部：五条路（trigger / person / recent / keyword /
    # embedding）只有汇合之处是唯一的，放这里不会漏掉任何一条路。
    if config.privacy.enabled and candidates:
        await attach_scope_info(candidates)
        scope = config.recall_scope
        gist_missing = str(config.privacy.gist_missing or "drop").strip().lower()
        filtered: list[RecallCandidate] = []
        dropped = 0
        for candidate in candidates:
            if not scope_allows(
                candidate,
                current_stream=current_stream,
                current_type=current_type,
                scope=scope,
            ):
                dropped += 1
                continue
            if is_cross_stream(
                candidate,
                current_stream=current_stream,
                current_type=current_type,
            ):
                # 跨流：换成外流版。没有外流版就按 gist_missing 处理——
                # 默认 drop：宁可这次少想起一条，也不让完整正文离开它的来源流。
                if candidate.gist:
                    candidate.content = candidate.gist
                elif gist_missing == "raw":
                    logger.warning(
                        f"[context_archiver] 记忆 {candidate.memory_id[:8]} 缺外流版，"
                        f"按 gist_missing=raw 原样跨流注入（不建议）"
                    )
                else:
                    dropped += 1
                    continue
            filtered.append(candidate)
        candidates = filtered
        if dropped and config.plugin.debug_log:
            logger.info(
                f"[context_archiver] 隐私分层拦截 {dropped} 条"
                f"（当前流 {current_type or '?'}，stream={current_stream[:8]}）"
            )

    # 去重（同 id 保留分最高的一条）
    blocked = exclude_ids or set()
    best: dict[str, RecallCandidate] = {}
    for candidate in candidates:
        if not candidate.memory_id or candidate.memory_id in blocked:
            continue
        current = best.get(candidate.memory_id)
        if current is None or candidate.score > current.score:
            best[candidate.memory_id] = candidate

    ordered = sorted(best.values(), key=lambda c: -c.score)[: max(1, int(recall_cfg.top_k))]
    if not ordered:
        return RecallOutcome(sources=sources)

    # 取全文，替换掉截断的 snippet
    contents = await _fetch_contents(service, [c.memory_id for c in ordered])
    for candidate in ordered:
        full = contents.get(candidate.memory_id)
        if full:
            candidate.content = full
        # 取全文会把内容换成完整正文 —— 跨流的那几条必须**再换回外流版**，
        # 否则就成了「先脱敏、后又被还原」，前面的拦截全白做。
        # 注意这个 `config.privacy.enabled` 守卫：关掉总开关时行为必须**完全**回到
        # 1.1.x（正文照旧跨流），不能因为候选恰好带着 gist 就偷偷脱敏。
        if config.privacy.enabled and candidate.gist and is_cross_stream(
            candidate,
            current_stream=current_stream,
            current_type=current_type,
        ):
            candidate.content = candidate.gist

    block = build_block(ordered, max_chars=int(recall_cfg.max_chars))
    if not block:
        return RecallOutcome(sources=sources)

    injected = [c.memory_id for c in ordered]
    if recall_cfg.touch_activated:
        await _touch_activated(service, injected)

    return RecallOutcome(
        block=block,
        injected_ids=injected,
        sources=sources,
        chars=len(block),
    )


__all__ = [
    "BOOKU_SIGNATURE",
    "RecallCandidate",
    "RecallOutcome",
    "attach_scope_info",
    "build_block",
    "extract_keywords",
    "extract_query_text",
    "is_cross_stream",
    "normalize_for_match",
    "recall_for_prompt",
    "scope_allows",
]
