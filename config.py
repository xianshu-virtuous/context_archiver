"""context_archiver 插件配置。

配置文件默认路径：``config/plugins/context_archiver/config.toml``。

设计约定（三条都是刻意的默认值，别随手改）：

1. ``plugin.mode`` 默认 ``observer``。它只观测、只记录、只打日志，
   不总结、不写记忆、不清空上下文。先用几天数据看清「话题到底什么时候结束」，
   再切 ``active``。
2. ``archive.clear_context_enabled`` 默认 ``false``。清空是**不可逆**的
   （框架写的是 ``context_cleared_at`` 水位线，重启后依然生效），
   所以它必须是用户显式打开的第二个开关，不能跟着 mode 一起开。
3. ``trigger.use_time_sense`` 默认 ``true``，但 **time_sense 不是依赖**：
   manifest 里没有它，取不到就自动降级为「闲置时长」判据。
"""

from __future__ import annotations

from typing import ClassVar

from src.app.plugin_system.base import BaseConfig, Field, SectionBase, config_section


class ContextArchiverConfig(BaseConfig):
    """上下文归档器配置模型。"""

    name: ClassVar[str] = "config"
    description: ClassVar[str] = "上下文归档器配置"

    @config_section("plugin")
    class PluginSection(SectionBase):
        """插件基础配置。"""

        enabled: bool = Field(
            default=True,
            description="是否启用本插件（false 时组件整体下线，不观察、不归档）",
        )
        mode: str = Field(
            default="observer",
            description=(
                "运行模式：\n"
                "observer —— 只观测：记录每流的活跃时间、结束信号与消息量，"
                "判定结果只写日志，不总结、不写记忆、不清空；\n"
                "active —— 执行：达到判定条件时真正总结、写记忆（是否清空另看 archive.clear_context_enabled）。"
            ),
        )
        debug_log: bool = Field(
            default=False,
            description="是否输出调试日志（活跃刷新、判定过程、注入文本）",
        )

    @config_section("trigger")
    class TriggerSection(SectionBase):
        """「什么时候算话题结束」的判定配置。"""

        tick_seconds: int = Field(
            default=15,
            description=(
                "判定巡检间隔（秒）。\n"
                "必须明显小于 idle_seconds，否则「静默 30 秒」要等下一次巡检才被发现，"
                "实际会变成「静默 30~45 秒」。默认 15。"
            ),
            ge=5,
            le=3600,
        )
        settle_seconds: int = Field(
            default=90,
            description=(
                "收到结束信号（stop_conversation）后，再等多少秒才归档。\n"
                "留这段时间是让最后几条消息真正落库，避免总结漏掉尾巴。"
            ),
            ge=0,
            le=3600,
        )
        idle_seconds: int = Field(
            default=30,
            description=(
                "**Bot 停口多久就算这一轮过去了**（秒）。\n"
                "这是写记忆的主阈值：**Bot 上次发言**之后静默达到它，就总结、写记忆。\n"
                "基准是「Bot 有没有参与」，不是「流有没有消息」——群聊里别人一直说话、"
                "话痨的 Bot 也一直在接话，按「流安静」判就永远不会触发。\n"
                "默认 30 秒：短到能插进话痨 Bot 的发言间隙，又长到不至于把半句话切断。\n"
                "配合 trigger.tick_seconds（默认 15）一起用。"
            ),
            ge=5,
            le=86400,
        )
        min_messages: int = Field(
            default=4,
            description=(
                "本次待归档消息少于多少条就不折腾。\n"
                "短寒暄不值得花一次模型调用，也不值得写一条记忆。"
            ),
            ge=1,
            le=1000,
        )
        max_messages: int = Field(
            default=400,
            description="单次最多取多少条消息参与总结（超出部分只取最近的，靠滚动摘要兜住更早内容）。",
            ge=10,
            le=5000,
        )

        use_time_sense: bool = Field(
            default=True,
            description=(
                "检测到 time_sense 时，用它的时间事实交叉验证。\n"
                "time_sense 2.0 起能按流查「这个流安静了多久」并能列出各流状态，"
                "本插件会自动用它；老版本（1.x）只有全局口径，同样能跑，"
                "只是「别处」与「本流」分不开。time_sense 不存在时自动降级，本项无副作用。"
            ),
        )
        require_global_quiet: bool = Field(
            default=False,
            description=(
                "是否要求「别的聊天流也安静」才归档。**默认关闭。**\n"
                "判定必须**按流独立进行**：只要这个流里有人发消息，它就是活跃的——\n"
                "不管那句话说给谁听、Bot 有没有参与。群里大家自顾自聊天时这个流是活跃的，"
                "当然不该归档；等这个流安静下来就该归档，**与别的流在不在聊无关**。\n"
                "打开它会造成「A 群在聊 → B 群安静了也写不了」，一般不需要。\n"
                "打开后的判定：time_sense 2.0 在位时按流核对（本流静默 + 其它流都静默），"
                "老版本则退回全局时间戳（任意流说话都会刷新它）。"
            ),
        )
        turn_trigger_enabled: bool = Field(
            default=True,
            description=(
                "按消息条数触发：该流累积够 turn_threshold 条消息就归档一次，"
                "不等静默、不管话题有没有结束。\n"
                "它与 trigger.idle_seconds 是**「或」**的关系，满足任意一个就归档：\n"
                "  · 攒够 turn_threshold 条（默认 100）→ 归档\n"
                "  · Bot 停口 idle_seconds 秒（默认 30）+ 消息数够 min_messages → 归档\n"
                "为什么要「或」：极活跃的群里 30 秒静默可能永远不出现，"
                "而话痨的 Bot 也让「参与静默」很难成立——条数这条兜住它们。\n"
                "轮数触发**只写记忆、绝不清空上下文**（话题可能还在继续）。"
            ),
        )
        turn_threshold: int = Field(
            default=100,
            description=(
                "累积多少条消息触发一次条数归档。\n"
                "调小＝写得更勤、花的模型调用更多；调大＝省调用但记忆更粗。"
            ),
            ge=2,
            le=5000,
        )

    @config_section("archive")
    class ArchiveSection(SectionBase):
        """归档动作配置。"""

        clear_context_enabled: bool = Field(
            default=False,
            description=(
                "是否在归档成功后清空该流上下文。\n"
                "默认关闭：清空写的是持久水位线（重启后依然生效），"
                "消息记录还在数据库里，但默认不再进入对话上下文。\n"
                "确认观察数据可靠后再打开。"
            ),
        )
        inject_summary_after_clear: bool = Field(
            default=True,
            description=(
                "清空后把该流的滚动摘要注入为背景（走 extra，跟历史消息一起进 user prompt）。\n"
                "这是「清空但不忘事」的关键：清掉原始消息，留下自己写过的摘要。"
            ),
        )
        summary_max_chars: int = Field(
            default=1200,
            description="滚动摘要最长保留多少字符（超出时让模型压缩重写）。",
            ge=200,
            le=8000,
        )
        max_digest_chars: int = Field(
            default=24000,
            description="喂给模型的对话文本上限（字符），超出只取最近的。",
            ge=1000,
            le=200000,
        )
        retry_backoff_seconds: int = Field(
            default=60,
            description=(
                "**失败退避**：一次归档失败后，这个流至少等多少秒才重试。\n"
                "失败不推进水位线（消息不会丢），但下一个 tick 会立刻重试——巡检间隔默认 15s，"
                "等于每 15 秒烧一次模型调用。实测某个流因此连续失败 **406 次**、"
                "一条记忆都没写出来，钱全花在重试上。\n"
                "退避按指数增长：60 → 120 → 240 → 480…（上限看 retry_backoff_max_seconds）。\n"
                "填 0 关闭退避（恢复「每个 tick 都试」的旧行为，不建议）。"
            ),
            ge=0,
            le=3600,
        )
        retry_backoff_max_seconds: int = Field(
            default=1800,
            description="退避的上限（秒）。默认 1800 ＝最慢每 30 分钟试一次。",
            ge=0,
            le=86400,
        )
        retry_warn_after: int = Field(
            default=3,
            description=(
                "连续失败达到几次时打一条 WARNING 日志（带最后一次的错误原因）。\n"
                "INFO 级的『巡检动作』很容易被刷过去，这条是给人看的告警。"
            ),
            ge=1,
            le=100,
        )

    @config_section("model")
    class ModelSection(SectionBase):
        """总结所用模型（沿用框架任务名体系）。"""

        task_name: str = Field(
            default="actor",
            description="模型任务名，默认跟主回复模型一致；填 model_name 时以它为准。",
        )
        model_name: str = Field(
            default="",
            description="直接点名某个模型（如 deepseek-v4-flash）；留空则用 task_name。",
        )
        temperature: float = Field(
            default=0.3,
            description="采样温度。总结要稳，别太高。",
            ge=0.0,
            le=2.0,
        )
        max_tokens: int = Field(
            default=8192,
            description=(
                "输出上限（摘要 + 记忆条目一起）。\n"
                "**思考链模型必须给足**：实测某个模型光是思考链就吃掉 3000~6200 token，"
                "上限太小会让正文（JSON）根本没机会输出——那不是「总结得短」，是「什么都没总结」。\n"
                "默认 8192。用非思考模型可以调回 2000 左右。"
            ),
            ge=400,
            le=32768,
        )

    @config_section("memory")
    class MemorySection(SectionBase):
        """记忆写入（sink）配置。"""

        sink: str = Field(
            default="booku",
            description=(
                "记忆写到哪儿：\n"
                "booku —— 写进框架自带 booku_memory（可被 RAG 检索到）；\n"
                "local —— 只写本插件自己的 json（booku_memory 不在时用）；\n"
                "none  —— 只总结、不落记忆（配合清空时要小心）。"
            ),
        )
        folder_id: str = Field(
            default="",
            description="记忆文件夹 id，留空用 booku_memory 自己的默认文件夹。",
        )
        status: str = Field(
            default="active",
            description=(
                "写入记忆的状态：active / archived。\n"
                "active 进隐现层，日常检索能搜到，但 7 天内被激活少于 2 次的会被 booku 自动丢弃；\n"
                "archived 进归档层，不会被淘汰，但默认检索不搜归档层（除非开闪回）。\n"
                "两种取舍，按你的用法选。"
            ),
        )
        bucket: str = Field(
            default="memory",
            description="存储桶，只有 memory / knowledge 两个有效值（其它值会被归一到 memory）。",
        )
        core_tags: list[str] = Field(
            default_factory=lambda: ["对话归档"],
            description=(
                "记忆的核心标签。\n"
                "booku_memory 要求 core/diffusion/opposing 三个标签都非空，"
                "模型没给出标签时用这里的兜底值。"
            ),
        )
        diffusion_tags: list[str] = Field(
            default_factory=lambda: ["聊天记录", "上下文归档"],
            description="扩散标签（兜底值）。",
        )
        opposing_tags: list[str] = Field(
            default_factory=lambda: ["临时", "闲聊"],
            description="对立标签（兜底值）。",
        )
        attach_person: bool = Field(
            default=True,
            description="有明确对话者时，把记忆挂到该人物（person_id）上，便于按人检索。",
        )
        touch_on_write: bool = Field(
            default=True,
            description=(
                "写入记忆后立刻把激活计数抬 1。\n"
                "**默认必须开**：booku 的检索按「最近激活」排序，新写的记忆激活数是 0，"
                "永远排不上队、召不回来，7 天后还会被隐现层当没用的丢掉——这是死锁。\n"
                "抬 1 之后召回时再 +1 就到 2，正好够晋升阈值。"
            ),
        )

    @config_section("recall")
    class RecallSection(SectionBase):
        """读取侧：自动召回（不需要 Bot 主动调 tool 就能记起事情）。

        两条路线，可单选可并用：

        - ``structured``（**默认**）：人物路 + 近因路 + 可选关键词路。
          全部走 booku 元数据层的 SQL 查询，**零网络、零 embedding、毫秒级**，
          所以可以直接在 prompt 构建里同步做，不增加首字延迟。
          代价：不做语义扩展（"饿了" 匹配不到 "没吃饭"）。
        - ``embedding``：走 ``retrieve_memories``（EPA 向量检索）。
          能命中语义相近的内容，代价是每轮一次 embedding 网络调用。
        - ``both``：两路都跑、合并去重。

        两条路线都是**绕过 tool** 的：Bot 不需要调用任何记忆工具，
        插件在 prompt 构建时直接把该记得的塞进上下文。
        """

        enabled: bool = Field(
            default=True,
            description=(
                "是否开启自动召回。\n"
                "开启后每轮往 user prompt 的 extra 里注入少量记忆（默认 ≤900 字符 ≈ 600 token）。\n"
                "成本约 0.002 元/轮；而它省掉的一次记忆 tool 调用值 0.035 元/轮——净赚。"
            ),
        )
        mode: str = Field(
            default="structured",
            description=(
                "召回路线：\n"
                "trigger —— 只走触发词路（存算一体：写入时算好的触发词 + 本地包含匹配）；\n"
                "structured（默认）—— 触发词路 + 人物/近因/关键词路（本地 SQL）；\n"
                "embedding —— 只走向量路（每轮一次 embedding 网络调用）；\n"
                "both —— 全都走。"
            ),
        )
        top_k: int = Field(
            default=3,
            description="最多注入几条记忆。条数越多越贵，3 条通常够。",
            ge=1,
            le=20,
        )
        max_chars: int = Field(
            default=900,
            description="注入文本的总字符上限（超出按相关度截断）。",
            ge=100,
            le=8000,
        )
        cooldown_seconds: int = Field(
            default=600,
            description=(
                "同一条记忆在这段时间内不重复注入。\n"
                "不做冷却的话，同一段事会被每轮重复塞进上下文——纯浪费。"
            ),
            ge=0,
            le=86400,
        )
        include_archived: bool = Field(
            default=True,
            description="是否检索归档层。默认开：本插件写入的记忆可能被标成 archived。",
        )
        person_first: bool = Field(
            default=True,
            description="人物路：优先取「关于当前对话者」的记忆（按 person_id 精确匹配）。",
        )
        recent_limit: int = Field(
            default=2,
            description="近因路：再补几条「最近被激活过的」记忆（不需要关键词，最稳）。",
            ge=0,
            le=10,
        )
        keyword_enabled: bool = Field(
            default=False,
            description=(
                "关键词路：从最近对话里抠词，走 grep 做字符级匹配。\n"
                "默认关——中文没有分词，抠词容易出噪音；等人物路+近因路不够用再开。"
            ),
        )
        keyword_limit: int = Field(
            default=2,
            description="关键词路最多贡献几条。",
            ge=0,
            le=10,
        )
        touch_activated: bool = Field(
            default=True,
            description=(
                "召回命中后调用 update_activated 抬高激活计数。\n"
                "**这是关键一环**：booku 的隐现层会在 7 天内淘汰激活不足的记忆，"
                "而被召回过的东西本来就该留下——读得越多，记忆越稳。"
            ),
        )
        trigger_enabled: bool = Field(
            default=True,
            description=(
                "触发词路（存算一体的读半）总开关。\n"
                "写记忆时让总结模型顺手吐出 3~8 个触发词（同义词/场景/物件），存进本地索引；\n"
                "之后每轮拿当前对话跟这张表做**字符串包含匹配**——零 LLM、零网络、零 tool。\n"
                "例：记忆「水壶昨天坏了有点漏电」触发词含「喝水/渴/水壶」，"
                "用户一句「有点想喝水」就能把那条捞上来。"
            ),
        )
        trigger_limit: int = Field(
            default=6,
            description="触发词路最多贡献几条候选（多了会被 top_k 再裁一次，这里只是上限）。",
            ge=0,
            le=30,
        )
        trigger_risk_bonus: float = Field(
            default=1.5,
            description="风险标记为 high（安全/健康/承诺/钱）的记忆加分，让它优先压过闲聊记忆。",
            ge=0.0,
            le=10.0,
        )
        trigger_person_bonus: float = Field(
            default=1.0,
            description="触发词命中且记忆本来就属于当前对话者时加分（群聊里 person 为空，不生效）。",
            ge=0.0,
            le=10.0,
        )
        trigger_half_life_hours: float = Field(
            default=72.0,
            description=(
                "触发词路的偏好半衰期（小时）：得分 × 0.5 ** (记忆年龄 / 这个值)。\n"
                "默认 72 小时——新鲜的事更容易被想起来，但旧事不会完全不出现。"
            ),
            ge=1.0,
            le=8760.0,
        )
        embedding_top_k: int = Field(
            default=5,
            description="embedding 路线先召回多少条候选（再由预算裁剪）。",
            ge=1,
            le=50,
        )
        timeout_seconds: float = Field(
            default=3.0,
            description=(
                "单次召回的时间上限（秒）。超时就放弃本轮注入，绝不让记忆拖慢对话。\n"
                "structured 路线是本地 SQL，正常几十毫秒。"
            ),
            ge=0.2,
            le=30.0,
        )
        target_prompts: list[str] = Field(
            default_factory=lambda: [
                "default_chatter_user_prompt",
                "neo_default_chatter_user_prompt",
                "kfc_user_prompt",
            ],
            description=(
                "允许注入召回的 user prompt 模板名列表。\n"
                "默认覆盖 default_chatter、neo_default_chatter 与 kokoro_flow_chatter"
                "（NFC 借前者的注入点，因此同样命中）。"
            ),
        )

    @config_section("audit")
    class AuditSection(SectionBase):
        """审计与回滚记录配置。"""

        keep_records: int = Field(
            default=200,
            description=(
                "保留最近多少条归档审计。\n"
                "每条记：流、时间区间、消息条数、摘要、写入的 memory_id、是否清空。\n"
                "清空不可逆，这份记录是事后追查「那天到底总结了什么」的唯一依据。"
            ),
            ge=10,
            le=5000,
        )
        prompt_audit_enabled: bool = Field(
            default=True,
            description=(
                "是否逐轮记录 user prompt 各板块的字符数（history / unreads / extra）。\n"
                "用来回答「钱花在哪」：实测每轮输入里全价（未命中缓存）的部分约 8,561 token、"
                "占总成本 74%，而且与历史长度无关——那它只可能来自每轮变化的内容。\n"
                "只读统计，不改动 prompt，不拦截事件。"
            ),
        )
        action_stats_enabled: bool = Field(
            default=True,
            description=(
                "是否统计动作/工具的调用次数。\n"
                "实测一次「对话回合」平均触发 5.56 次 LLM 请求、每多一步多花 0.0350 元，"
                "所以要省钱就得知道这 5.56 步花在哪些动作上。只读统计。"
            ),
        )
        input_audit_enabled: bool = Field(
            default=True,
            description=(
                "是否在 ``BEFORE_LLM_REQUEST`` 上统计**真正的输入构成**。\n"
                "``ON_PROMPT_BUILD`` 只能看见模板占位符（实测只占单次输入 1.7%），"
                "完整输入在 payloads + tools 里：单次请求约 59k token，其中约 41k 是"
                "system prompt 与工具/动作声明。这里把「工具声明」和「各角色 payload」分开数，"
                "用来定「砍哪一块最值」。框架允许订阅者改 payloads/tools，"
                "所以这份数据直接对应「能裁多少」。只读，不改。"
            ),
        )
        input_audit_request_names: list[str] = Field(
            default_factory=list,
            description=(
                "只统计这些请求名；**留空表示全部统计**（默认）。\n"
                "留空更安全：实测请求名不一定就是 ``default_chatter``，"
                "写死会把真正的调用整个漏掉（这个坑踩过一次）。\n"
                "统计里会按 ``req:<请求名>`` 分组，所以混着也不会看不清。"
            ),
        )

    @config_section("observer")
    class ObserverSection(SectionBase):
        """observer 模式的观测参数。"""

        log_every_seconds: int = Field(
            default=1800,
            description="observer 模式下，同一流的观测日志最小间隔（秒），避免刷屏。",
            ge=60,
            le=86400,
        )
        log_skipped_short: bool = Field(
            default=False,
            description="是否连「消息太少不值得归档」的判定也打日志。",
        )

    plugin: PluginSection = Field(default_factory=PluginSection)
    trigger: TriggerSection = Field(default_factory=TriggerSection)
    archive: ArchiveSection = Field(default_factory=ArchiveSection)
    model: ModelSection = Field(default_factory=ModelSection)
    memory: MemorySection = Field(default_factory=MemorySection)
    recall: RecallSection = Field(default_factory=RecallSection)
    audit: AuditSection = Field(default_factory=AuditSection)
    observer: ObserverSection = Field(default_factory=ObserverSection)


__all__ = ["ContextArchiverConfig"]
