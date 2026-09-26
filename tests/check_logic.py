# -*- coding: utf-8 -*-
r"""逻辑自检：不依赖框架运行时，把「最容易悄悄出错」的几处逐个钉住。

重点覆盖三块：

1. **分页与截断**（``collect_messages``）——``get_stream_messages`` 是按 ``-id``
   倒序返回的，只取一页会漏掉更早的未归档消息，而这些消息会在水位线推进后
   被永久跳过。这里用假的消息源复现三种情况：水位线截断、正常翻页、超上限截断。
2. **模型脏输出解析**（``extract_json``）——模型爱把 JSON 包在代码围栏里、
   用全角引号、写行注释、留尾逗号。
3. **标签兜底**（``MemoryItem.normalized``）——booku_memory 要求
   core/diffusion/opposing 三个标签都非空，缺一个就抛错。

用法（实例 venv）：

    $env:NEO_ROOT="F:\Neo-MoFox-Aemeath"
    & "F:\Neo-MoFox-Aemeath\.venv\Scripts\python.exe" tests\check_logic.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

#: 控制台默认可能是 GBK，而自检里会出现 ⚠ 这类非 GBK 字符——直接 print 会抛
#: UnicodeEncodeError 把整个自检带崩。统一强制 UTF-8（换不动的就退化成 replace）。
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
except Exception:  # noqa: BLE001 - 老解释器没有 reconfigure 就算了
    pass

PLUGIN_DIR = Path(__file__).resolve().parents[1]
NEO_ROOT = Path(os.environ.get("NEO_ROOT", r"F:\Neo-MoFox-Aemeath"))
sys.path.insert(0, str(NEO_ROOT))
sys.path.insert(0, str(PLUGIN_DIR.parent))

from context_archiver import archiver, llm, recall as rc, state as st  # noqa: E402
from context_archiver.config import ContextArchiverConfig  # noqa: E402
from context_archiver.sink import MemoryItem  # noqa: E402

failures: list[str] = []
checks = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    """记录一条断言结果。"""
    global checks
    checks += 1
    if condition:
        print(f"  OK    {label}")
    else:
        print(f"  FAIL  {label}  {detail}")
        failures.append(label)


def section(title: str) -> None:
    print()
    print("=" * 68)
    print(title)
    print("=" * 68)


# --------------------------------------------------------------------------- #
# 假消息源（按 framework 的真实行为：-id 倒序 = 最新在前）
# --------------------------------------------------------------------------- #


@dataclass
class FakeMessage:
    """最小可用的消息替身。"""

    time: float
    person_id: str = "person-a"
    content: str = "内容"
    message_id: str = ""


class FakeStreamApi:
    """假的 stream_api：按页返回「最新在前」的消息。"""

    def __init__(self, pages: list[list[FakeMessage]]) -> None:
        self._pages = pages
        self.calls: list[tuple[int, int]] = []

    async def get_stream_messages(
        self, stream_id: str, limit: int = 100, offset: int = 0
    ) -> list[FakeMessage]:
        """按 offset 取第几页。"""
        self.calls.append((limit, offset))
        index = offset // max(1, limit)
        if index >= len(self._pages):
            return []
        return list(self._pages[index])


def _desc_page(start_ts: float, count: int) -> list[FakeMessage]:
    """造一页消息：时间从 start_ts 递减，并带上可用的 message_id。"""
    return [
        FakeMessage(time=start_ts - i, message_id=f"id-{i}") for i in range(count)
    ]


# --------------------------------------------------------------------------- #
# 1. 分页与截断
# --------------------------------------------------------------------------- #


def test_collect_messages() -> None:
    """collect_messages 的水位线、翻页与截断行为。"""
    section("1. 消息读取：水位线 / 翻页 / 截断保护的")
    real_api = archiver.stream_api

    try:
        # A. 时间戳只当安全下界：比它旧的排除，取不到时间的宁可保留
        fake = FakeStreamApi([_desc_page(300.0, 3)])  # 300, 299, 298
        archiver.stream_api = fake
        messages, truncated = asyncio.run(
            archiver.collect_messages("s", waterline_ts=299.0, max_messages=400)
        )
        check("时间下界过滤生效（只取 > 299 的 1 条）", len(messages) == 1, f"实际 {len(messages)}")
        check("没取满不算截断", truncated is False)
        check("返回按时间升序", [m.time for m in messages] == sorted(m.time for m in messages))

        # A2. 时间取不到（0）时不能被下界误排除
        fake = FakeStreamApi([[FakeMessage(time=0.0, message_id="z1")]])
        archiver.stream_api = fake
        messages, _ = asyncio.run(
            archiver.collect_messages("s", waterline_ts=299.0, max_messages=400)
        )
        check("时间戳无效的消息不被误排除", len(messages) == 1, f"实际 {len(messages)}")

        # B. 只取一页（最新 cap 条），按 message_id 过滤已归档的
        page0 = _desc_page(1000.0, 100)  # 1000..901，带 id-0..id-99
        fake = FakeStreamApi([page0])
        archiver.stream_api = fake
        messages, truncated = asyncio.run(
            archiver.collect_messages("s", waterline_ts=0.0, max_messages=400)
        )
        check("一页取全（100 条）", len(messages) == 100, f"实际 {len(messages)}")
        check("只请求一次（不再翻页找边界）", len(fake.calls) == 1, f"请求 {fake.calls}")
        check("没取满不算截断", truncated is False)

        # B2. 已归档的 id 是**交错**分布的，也要能正确过滤（这是老实现最致命的错）
        blocked = {f"id-{i}" for i in range(0, 100, 2)}  # 偶数号已归档
        fake = FakeStreamApi([_desc_page(1000.0, 100)])
        archiver.stream_api = fake
        messages, _ = asyncio.run(
            archiver.collect_messages(
                "s", waterline_ts=0.0, exclude_ids=blocked, max_messages=400
            )
        )
        check(
            "交错分布下已归档的被全部过滤（剩 50 条）",
            len(messages) == 50,
            f"实际 {len(messages)}",
        )
        check(
            "过滤后剩下的 id 都不在已归档集合里",
            all(m.message_id not in blocked for m in messages),
        )

        # C. 超上限：必须报 truncated（调用方据此跳过清空）
        fake = FakeStreamApi([_desc_page(1000.0, 100)])
        archiver.stream_api = fake
        messages, truncated = asyncio.run(
            archiver.collect_messages("s", waterline_ts=0.0, max_messages=10)
        )
        check("超上限时截断标记为 True", truncated is True)
        check("超上限时保留最早的 10 条", len(messages) == 10, f"实际 {len(messages)}")
        check(
            "保留的是最早那批而非最新（否则更早的未归档消息会被永久跳过）",
            [m.time for m in messages] == sorted(m.time for m in messages)
            and min(m.time for m in messages) == 901.0,
            f"范围 {min(m.time for m in messages)}~{max(m.time for m in messages)}",
        )

        # D. 读不到消息时返回空，且不抛异常
        archiver.stream_api = FakeStreamApi([])
        messages, truncated = asyncio.run(
            archiver.collect_messages("s", waterline_ts=0.0, max_messages=400)
        )
        check("没有消息时返回空列表", messages == [] and truncated is False)
    finally:
        archiver.stream_api = real_api


# --------------------------------------------------------------------------- #
# 2. 模型脏输出解析
# --------------------------------------------------------------------------- #


def test_extract_json() -> None:
    """extract_json 对各种模型输出的容错。"""
    section("2. 模型输出解析（extract_json）")

    cases: list[tuple[str, str, dict | None]] = [
        ("干净的 JSON", '{"summary": "好", "memories": []}', {"summary": "好", "memories": []}),
        ("代码围栏包裹", '```json\n{"summary": "围栏"}\n```', {"summary": "围栏"}),
        ("围栏无语言标记", '```\n{"summary": "无标记"}\n```', {"summary": "无标记"}),
        ("全角引号与冒号", '｛"summary"："全角"｝', None),  # 全角括号不在翻译表内，预期失败
        ("前后有解释文字", '好的，这是结果：\n{"summary": "带前言"}\n以上。', {"summary": "带前言"}),
        ("带行注释", '{\n  // 说明\n  "summary": "注释"\n}', {"summary": "注释"}),
        ("带尾逗号", '{"summary": "尾逗号",}', {"summary": "尾逗号"}),
        ("非 JSON 文本", "我觉得这段对话没什么特别的。", None),
        ("空字符串", "", None),
    ]

    for label, text, expected in cases:
        got = llm.extract_json(text)
        if expected is None:
            check(f"{label} → 不误报", got is None, f"实际 {got}")
        else:
            check(f"{label} → 解析正确", got == expected, f"实际 {got}")


# --------------------------------------------------------------------------- #
# 3. 标签兜底（booku 要求三元组非空）
# --------------------------------------------------------------------------- #


def test_memory_item_normalized() -> None:
    """MemoryItem.normalized 的兜底行为。"""
    section("3. 记忆条目标签兜底（booku 三元组必填）")

    config = ContextArchiverConfig()

    empty = MemoryItem(title="", content="内容", memory_type="乱写的类型")
    normalized = empty.normalized(config)
    check("空标题被兜底", bool(normalized.title), f"标题={normalized.title!r}")
    check("core_tags 非空", bool(normalized.core_tags), f"{normalized.core_tags}")
    check("diffusion_tags 非空", bool(normalized.diffusion_tags), f"{normalized.diffusion_tags}")
    check("opposing_tags 非空", bool(normalized.opposing_tags), f"{normalized.opposing_tags}")
    check("非法 memory_type 被纠正", normalized.memory_type in {
        "event", "knowledge", "person", "place", "procedure"
    }, f"{normalized.memory_type}")

    given = MemoryItem(
        title="有标签",
        content="内容",
        memory_type="person",
        core_tags=["  ", ""],
        diffusion_tags=["a", "b"],
        opposing_tags=["c"],
    )
    normalized_given = given.normalized(config)
    check("全是空白的标签被兜底替换", normalized_given.core_tags != ["  ", ""])
    check("有内容的标签保持原样", normalized_given.diffusion_tags == ["a", "b"])
    check("正常 memory_type 不被改动", normalized_given.memory_type == "person")

    too_many = MemoryItem(
        title="t",
        content="c",
        core_tags=[f"tag{i}" for i in range(20)],
    ).normalized(config)
    check("标签数量被裁剪", len(too_many.core_tags) <= 8, f"{len(too_many.core_tags)}")


# --------------------------------------------------------------------------- #
# 4. 对话文本装配
# --------------------------------------------------------------------------- #


def test_build_digest() -> None:
    """build_digest 的截断与排序。"""
    section("4. 对话文本装配（build_digest）")

    empty = archiver.build_digest([], max_chars=1000)
    check("空消息列表 → 空文本", empty == "")

    messages = [FakeMessage(time=1_700_000_000 + i, content=f"第{i}句") for i in range(5)]
    text = archiver.build_digest(messages, max_chars=10000)
    check("包含全部句子", all(f"第{i}句" in text for i in range(5)))
    check("每条都带时间戳", text.count("[") == 5, f"出现 {text.count('[')} 次")

    long_messages = [FakeMessage(time=1_700_000_000 + i, content="长" * 200) for i in range(50)]
    truncated_text = archiver.build_digest(long_messages, max_chars=1000)
    check("超长时被截断到上限附近", len(truncated_text) <= 1400, f"实际 {len(truncated_text)}")
    check("截断时保留的是最近的内容（有省略提示）", "省略" in truncated_text)


# --------------------------------------------------------------------------- #
# 5. 判定边界
# --------------------------------------------------------------------------- #


def test_evaluate_edges() -> None:
    """evaluate 的边界。"""
    section("5. 判定边界（evaluate）")

    config = ContextArchiverConfig()
    config.trigger.min_messages = 4
    config.trigger.idle_seconds = 1800
    config.trigger.settle_seconds = 90
    now = 1_000_000.0

    # settle_seconds = 0 时，结束信号立刻生效
    config.trigger.settle_seconds = 0
    decision = archiver.evaluate(
        st.StreamState(
            stream_id="x",
            pending_count=10,
            last_activity_at=now - 1,
            end_signal_at=now - 1,
            end_signal_name="stop_conversation",
        ),
        config,
        now=now,
    )
    check("settle=0 时结束信号立即触发", decision.should is True and decision.trigger == "end_signal")

    config.trigger.settle_seconds = 90
    # 已处理过的结束信号不该重复触发（应退回闲置判定）
    decision = archiver.evaluate(
        st.StreamState(
            stream_id="y",
            pending_count=10,
            last_activity_at=now - 5,
            end_signal_at=now - 500,
            last_settled_signal_at=now - 500,
        ),
        config,
        now=now,
    )
    check("已消费的结束信号不重复触发", decision.should is False, f"{decision.reason}")

    # 完全没有活跃记录，且待归档不多 → 不归档
    decision = archiver.evaluate(
        st.StreamState(stream_id="z", pending_count=3), config, now=now
    )
    check("没有活跃记录且消息不足 → 不归档", decision.should is False)

    # ★ 核心场景 A：群聊一直有人说话（last_activity 刚刷新），但 Bot 潜水很久
    #    —— 判定基准必须是「Bot 有没有参与」，否则群聊永远不会归档。
    decision = archiver.evaluate(
        st.StreamState(
            stream_id="g",
            pending_count=50,
            last_activity_at=now - 5,
            last_engagement_at=now - 2000,
        ),
        config,
        now=now,
    )
    check(
        "群聊热闹但 Bot 潜水超阈值 → 归档",
        decision.should is True and decision.trigger == "idle",
        f"{decision.should} / {decision.trigger} / {decision.reason}",
    )

    # ★ 核心场景 B：流安静很久，但 Bot 刚说过话 → 不该归档
    decision = archiver.evaluate(
        st.StreamState(
            stream_id="h",
            pending_count=50,
            last_activity_at=now - 3000,
            last_engagement_at=now - 60,
        ),
        config,
        now=now,
    )
    check(
        "Bot 刚参与过 → 不归档（哪怕流安静很久）",
        decision.should is False,
        decision.reason,
    )

    # 轮数触发默认关闭；显式打开后才生效
    turn_threshold = int(config.trigger.turn_threshold)
    config.trigger.turn_trigger_enabled = True
    decision = archiver.evaluate(
        st.StreamState(
            stream_id="t",
            pending_count=turn_threshold,
            last_activity_at=now - 5,
            last_engagement_at=now - 5,
        ),
        config,
        now=now,
    )
    check(
        "打开轮数触发后，累积够 → 轮数归档",
        decision.should is True and decision.trigger == "turns",
        f"{decision.should} / {decision.trigger}",
    )

    config.trigger.turn_trigger_enabled = False
    decision = archiver.evaluate(
        st.StreamState(
            stream_id="v",
            pending_count=turn_threshold * 3,
            last_activity_at=now - 5,
            last_engagement_at=now - 5,
        ),
        config,
        now=now,
    )
    check("默认关闭时不做轮数归档", decision.should is False, decision.reason)

    # 结束信号优先于「仍然活跃」
    decision = archiver.evaluate(
        st.StreamState(
            stream_id="w",
            pending_count=10,
            last_activity_at=now - 1,
            end_signal_at=now - 100,
            end_signal_name="stop_conversation",
        ),
        config,
        now=now,
    )
    check("刚说完话但有结束信号 → 仍归档", decision.should is True and decision.trigger == "end_signal")


# --------------------------------------------------------------------------- #
# 6. 审计记录序列化往返
# --------------------------------------------------------------------------- #


def test_audit_roundtrip() -> None:
    """审计记录 to_dict/from_dict 往返不丢字段。"""
    section("6. 审计记录序列化往返")

    record = st.ArchiveAuditRecord(
        at=1.0,
        stream_id="s1",
        trigger="end_signal",
        message_count=12,
        start_ts=10.0,
        end_ts=20.0,
        summary="摘要内容",
        memory_ids=["m1", "m2"],
        sink="booku",
        cleared=True,
        ok=True,
        error="",
    )
    restored = st.ArchiveAuditRecord.from_dict(record.to_dict())
    check("stream_id 保留", restored.stream_id == "s1")
    check("memory_ids 保留", restored.memory_ids == ["m1", "m2"])
    check("cleared 保留", restored.cleared is True)
    check("message_count 保留", restored.message_count == 12)

    broken = st.ArchiveAuditRecord.from_dict(None)
    check("异常输入退回默认值", broken.stream_id == "" and broken.ok is False)

    weird = st.ArchiveAuditRecord.from_dict(
        {"memory_ids": "not-a-list", "message_count": "abc", "cleared": 1}
    )
    check("字段类型异常被容错", weird.memory_ids == [] and weird.message_count == 0)


# --------------------------------------------------------------------------- #
# 7. 存算一体：触发词路（写入时预计算，读取时本地包含匹配）
# --------------------------------------------------------------------------- #


def test_trigger_route() -> None:
    """触发词路：那张「水壶 / 渴」的经典例子必须真的能命中。"""
    section("7. 存算一体（触发词路，零 LLM 查询）")

    # 必须用真实时钟：_recall_triggers 内部按 time.time() 算年龄，写死一个
    # 过去的常量会让 0.5 ** (小时数/半衰期) 直接下溢成 0 分。
    now = time.time()
    index = [
        {
            "id": "m-kettle",
            "title": "水壶坏了",
            "snippet": "水壶昨天就坏了，有点漏电，先别用",
            "triggers": ["水壶", "喝水", "渴", "烧水", "漏电"],
            "risk": "high",
            "person": "u1",
            "at": now - 3600.0,
        },
        {
            "id": "m-milk",
            "title": "买牛奶",
            "snippet": "主人说想喝牛奶",
            "triggers": ["牛奶", "早餐"],
            "risk": "normal",
            "person": "u1",
            "at": now - 3600.0 * 48,
        },
        {
            "id": "m-noise",
            "title": "单字噪音",
            "snippet": "只有一个单字触发词",
            "triggers": ["水"],
            "risk": "normal",
            "person": "",
            "at": now,
        },
        {
            "id": "m-two-single",
            "title": "两个单字",
            "snippet": "两个单字触发词够成佐证",
            "triggers": ["水", "喝"],
            "risk": "normal",
            "person": "",
            "at": now,
        },
    ]

    real_load = st.load_memory_index
    real_get_booku = rc._get_booku

    async def fake_load() -> list[dict]:
        return [dict(item) for item in index]

    class FakeBooku:
        """只实现召回路径用到的两个方法。"""

        async def read_full_content(self, memory_ids: list[str]) -> dict:
            return {"items": []}

        async def update_activated(self, memory_id: str) -> None:
            return None

    st.load_memory_index = fake_load  # type: ignore[assignment]

    async def fake_get_booku():
        return FakeBooku()

    rc._get_booku = fake_get_booku  # type: ignore[assignment]

    try:
        check("normalize_for_match 去空白并小写", rc.normalize_for_match(" 好 渴 A ") == "好渴a")

        config = ContextArchiverConfig()
        config.recall.trigger_limit = 6
        config.recall.trigger_half_life_hours = 72.0
        config.recall.cooldown_seconds = 0
        check("触发词路默认开启", config.recall.trigger_enabled is True)

        query = "好渴啊，有点想喝水"
        hits = asyncio.run(rc._recall_triggers(config, query, person_id="u1"))
        ids = [c.memory_id for c in hits]
        check("「渴 / 喝水」命中水壶那条", "m-kettle" in ids, f"{ids}")
        check("无关记忆不命中", "m-milk" not in ids, f"{ids}")
        check("单字触发词被当噪音丢弃", "m-noise" not in ids, f"{ids}")
        check("两个单字命中算够佐证 → 保留", "m-two-single" in ids, f"{ids}")
        check(
            "多字命中排在单字组合之前",
            ids.index("m-kettle") < ids.index("m-two-single"),
            f"{ids}",
        )

        kettle = next(c for c in hits if c.memory_id == "m-kettle")
        check("候选正文先用索引里的 snippet 占位", bool(kettle.content), kettle.content[:20])
        check("risk=high 被打上标记", kettle.risk == "high")
        check(
            "打分包含 risk/person 加分（>4+1+1.5 衰减后仍 >7）",
            kettle.score > 7.0,
            f"{kettle.score:.3f}",
        )

        no_person = asyncio.run(rc._recall_triggers(config, query, person_id=""))
        kettle_np = next(c for c in no_person if c.memory_id == "m-kettle")
        check("人物一致有加分", kettle.score > kettle_np.score, f"{kettle.score:.3f} vs {kettle_np.score:.3f}")

        # 时间衰减：同一条记忆，写的时间越久远分越低
        old_index = [dict(index[0], at=now - 3600.0 * 24 * 30)]
        st.load_memory_index = lambda: _async_list(old_index)  # type: ignore[assignment]
        old_hits = asyncio.run(rc._recall_triggers(config, query, person_id="u1"))
        check("时间衰减：30 天前的同一条分更低", old_hits[0].score < kettle.score, f"{old_hits[0].score:.3f}")
        st.load_memory_index = fake_load  # type: ignore[assignment]

        config.recall.trigger_limit = 0
        check("trigger_limit=0 → 不贡献候选", asyncio.run(rc._recall_triggers(config, query)) == [])
        config.recall.trigger_limit = 6

        config.recall.trigger_enabled = False
        outcome_off = asyncio.run(
            rc.recall_for_prompt(config, {"unreads": query}, person_id="u1")
        )
        check("总开关关掉后不注入", outcome_off.block == "" and "trigger" not in outcome_off.sources)
        config.recall.trigger_enabled = True

        # 端到端：mode=trigger → 组装出可注入文本
        config.recall.mode = "trigger"
        config.recall.max_chars = 900
        outcome = asyncio.run(rc.recall_for_prompt(config, {"unreads": query}, person_id="u1"))
        check("mode=trigger 时确实注入了内容", bool(outcome.block), f"error={outcome.error}")
        check("注入内容带上了水壶这条", "水壶" in outcome.block, outcome.block[:80])
        check("高风险记忆带 ⚠ 前缀", "⚠" in outcome.block, outcome.block[:80])
        check("sources 记录了 trigger 路", outcome.sources.get("trigger", 0) >= 1, f"{outcome.sources}")
        check("injected_ids 可用于写冷却", "m-kettle" in outcome.injected_ids, f"{outcome.injected_ids}")

        # mode=structured 也应包含触发词路（本地零成本）
        config.recall.mode = "structured"
        config.recall.recent_limit = 0
        config.recall.person_first = False
        outcome2 = asyncio.run(rc.recall_for_prompt(config, {"unreads": query}, person_id="u1"))
        check("mode=structured 也走触发词路", "水壶" in outcome2.block, outcome2.block[:80])
    finally:
        st.load_memory_index = real_load  # type: ignore[assignment]
        rc._get_booku = real_get_booku  # type: ignore[assignment]


async def _async_list(items: list[dict]) -> list[dict]:
    """把一个列表包成 async 函数（给假的 load_memory_index 用）。"""
    return items


# --------------------------------------------------------------------------- #
# 8. 失败退避（不许每 15 秒重试一次烧钱）
# --------------------------------------------------------------------------- #


class _FakeStorage:
    """假的 storage_api：只记在内存里，够验证「失败计数真的落盘了」。"""

    def __init__(self) -> None:
        self.data: dict[tuple[str, str], object] = {}

    async def load_json(self, store_name: str, key: str):  # noqa: ANN201
        return self.data.get((store_name, key))

    async def save_json(self, store_name: str, key: str, value):  # noqa: ANN001, ANN201
        self.data[(store_name, key)] = value
        return True


def test_retry_backoff() -> None:
    """失败必须退避，而且失败计数要真的落盘（否则退避无从生效）。"""
    section("8. 失败退避（连续失败不再每 tick 重试）")

    config = ContextArchiverConfig()
    config.trigger.idle_seconds = 300
    config.trigger.min_messages = 20
    config.archive.retry_backoff_seconds = 60
    config.archive.retry_backoff_max_seconds = 1800

    curve = {n: archiver._backoff_seconds(n, config) for n in (0, 1, 2, 3, 5, 6, 20)}
    check("1 次失败退避 60s", curve[1] == 60.0, f"{curve[1]}")
    check("2 次失败退避 120s", curve[2] == 120.0, f"{curve[2]}")
    check("3 次失败退避 240s", curve[3] == 240.0, f"{curve[3]}")
    check("5 次失败退避 960s", curve[5] == 960.0, f"{curve[5]}")
    check("封顶 1800s（6 次）", curve[6] == 1800.0, f"{curve[6]}")
    check("封顶后不再涨（20 次）", curve[20] == 1800.0, f"{curve[20]}")
    check("0 次失败不退避", curve[0] == 0.0, f"{curve[0]}")

    off = ContextArchiverConfig()
    off.archive.retry_backoff_seconds = 0
    check("retry_backoff_seconds=0 时关闭退避", archiver._backoff_seconds(5, off) == 0.0)

    tiny = ContextArchiverConfig()
    tiny.archive.retry_backoff_seconds = 120
    tiny.archive.retry_backoff_max_seconds = 30
    check(
        "上限小于基数时按基数兜底（不会算成 0 或负数）",
        archiver._backoff_seconds(3, tiny) == 120.0,
        f"{archiver._backoff_seconds(3, tiny)}",
    )

    # 失败计数真的落盘吗（走真实 state 存取路径 + 假 storage）
    real_storage = st.storage_api
    st.storage_api = _FakeStorage()  # type: ignore[assignment]
    try:
        st.ArchiveStateStore.invalidate()

        state = st.StreamState(stream_id="s1", pending_count=31)
        asyncio.run(st.ArchiveStateStore.save_all({"s1": state}, force=True))

        asyncio.run(
            archiver._mark_failure(
                state, config, stream_id="s1", error="总结调用失败: empty response"
            )
        )
        after_one = asyncio.run(st.ArchiveStateStore.get("s1", fresh=True))
        # 先把读数取出来再制造第二次失败：_mark_failure 是就地修改传入对象的，
        # 直接把 after_one 传进去会让它自己也变成 2（第一次写这测试就踩了）。
        one_streak = after_one.fail_streak
        one_fail_at = after_one.last_fail_at

        asyncio.run(
            archiver._mark_failure(
                after_one, config, stream_id="s1", error="模型输出不是合法 JSON"
            )
        )
        after_two = asyncio.run(st.ArchiveStateStore.get("s1", fresh=True))

        check("第一次失败后 fail_streak=1 已落盘", one_streak == 1, f"{one_streak}")
        check("第二次失败后 fail_streak=2 已落盘", after_two.fail_streak == 2, f"{after_two.fail_streak}")
        check("last_fail_at 被记录", one_fail_at > 0)

        # 正常情况下「Bot 停了 500s」就是该归档，但退避期内必须压住
        now = time.time()
        after_one.last_engagement_at = now - 500.0
        after_two.last_engagement_at = now - 500.0
        reasons = [
            archiver.evaluate(item, config, now=now).reason
            for item in (after_one, after_two)
        ]
        check("退避期内判定为「不归档」", all("退避" in r for r in reasons), f"{reasons}")
        check("退避秒数按失败次数增长（2 次＝120s）", "120s" in reasons[1], reasons[1])

        # 退避到期后恢复正常判定
        expired = st.StreamState(
            stream_id="s2",
            pending_count=31,
            last_engagement_at=now - 500.0,
            fail_streak=1,
            last_fail_at=now - 61.0,
        )
        decision = archiver.evaluate(expired, config, now=now)
        check("退避期满后照常归档", decision.should is True, decision.reason)

        off.archive.retry_backoff_seconds = 0
        retry_now = archiver.evaluate(expired, off, now=now)
        check("关闭退避后立刻重试", retry_now.should is True, retry_now.reason)

        restored = st.StreamState.from_dict(after_two.to_dict())
        check(
            "fail_streak / last_fail_at 能往返",
            restored.fail_streak == 2 and restored.last_fail_at == after_two.last_fail_at,
            f"{restored.fail_streak} / {restored.last_fail_at}",
        )
    finally:
        st.storage_api = real_storage  # type: ignore[assignment]
        st.ArchiveStateStore.invalidate()


# --------------------------------------------------------------------------- #
# 入口
# --------------------------------------------------------------------------- #


def main() -> int:
    print(f"框架根  : {NEO_ROOT}")
    print(f"插件目录: {PLUGIN_DIR}")
    test_collect_messages()
    test_extract_json()
    test_memory_item_normalized()
    test_build_digest()
    test_evaluate_edges()
    test_audit_roundtrip()
    test_trigger_route()
    test_retry_backoff()

    section("结果")
    print(f"共 {checks} 项断言，失败 {len(failures)} 项")
    for item in failures:
        print(f"  - {item}")
    print()
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
