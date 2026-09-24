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
            default=60,
            description=(
                "判定巡检间隔（秒）。\n"
                "与 time_sense 的 heartbeat_seconds 对齐时联动最自然（默认都是 300 的整数分频）。"
            ),
            ge=10,
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
            default=1800,
            description=(
                "没有结束信号时的兜底：该流静默超过这么久，视为话题已停。\n"
                "调小会更早归档、省得多，但也更容易把「聊到一半去倒水」误判成结束。"
            ),
            ge=60,
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
                "检测到 time_sense 时，用它的「距上次说话」交叉验证。\n"
                "time_sense 不存在时自动降级，本项无副作用。"
            ),
        )
        require_global_quiet: bool = Field(
            default=True,
            description=(
                "要求「全局也安静」才归档。\n"
                "开启后：只要全局还有任何流在说话（time_sense.note_message 被刷新），"
                "就不归档——避免私聊静默时被群聊的活跃掩盖成误判。\n"
                "关闭后只看本流自己的活跃时间。"
            ),
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
            default=1200,
            description="输出上限（总结 + 记忆条目一起）。",
            ge=200,
            le=8000,
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
    audit: AuditSection = Field(default_factory=AuditSection)
    observer: ObserverSection = Field(default_factory=ObserverSection)


__all__ = ["ContextArchiverConfig"]
