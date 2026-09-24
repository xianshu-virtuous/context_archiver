"""context_archiver 读取侧：自动召回。

**要解决的问题**：Bot 想记起一件事，现在得自己调记忆工具——那是一次额外的
agent 循环，一次 0.0350 元，而且模型经常想不起来调。这个模块把它变成系统行为：
在 prompt 构建时自动把该记得的捞出来塞进上下文，**Bot 一次 tool 都不用调**。

两条路线（``recall.mode``）：

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
from dataclasses import dataclass, field
from typing import Any

from src.app.plugin_system.api import service_api
from src.app.plugin_system.api.log_api import get_logger

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
        memory_id: 记忆 id。
        title: 标题。
        content: 正文（可能是 snippet，取到全文后替换）。
        source: 来自哪条路（person / recent / keyword / embedding）。
        score: 排序分（越大越优先）。
    """

    memory_id: str = ""
    title: str = ""
    content: str = ""
    source: str = ""
    score: float = 0.0


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
以下是你记忆里与当前对话相关的内容（自动回想，不是让你念出来的台词，别复述原句）："""


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


async def recall_for_prompt(
    config: ContextArchiverConfig,
    values: dict[str, Any],
    *,
    exclude_ids: set[str] | None = None,
    person_id: str = "",
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

    service = await _get_booku()
    if service is None:
        return RecallOutcome(error="booku_memory 不可用")

    mode = str(recall_cfg.mode or "structured").strip().lower()
    query_text = extract_query_text(values)
    sources: dict[str, int] = {}

    async def _run() -> list[RecallCandidate]:
        collected: list[RecallCandidate] = []
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
    "build_block",
    "extract_keywords",
    "extract_query_text",
    "recall_for_prompt",
]
