"""context_archiver 模型调用层。

把「选模型 → 组请求 → 取文本 → 抠 JSON」收敛到一处，供总结与记忆抽取共用。

模型选择规则与 ``daily_schedule`` 保持一致（同一套任务名体系）：

1. ``model.model_name`` 非空 → ``llm_api.get_model_set_by_name`` 直接点名；
2. 否则用 ``model.task_name``（默认 ``actor``，即主回复模型）。

调用失败一律返回 ``LLMCallResult(ok=False)``，由调用方决定降级行为，
绝不向对话主流程抛异常。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from src.app.plugin_system.api import llm_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.types import LLMPayload, ROLE, Text

from .config import ContextArchiverConfig

logger = get_logger("context_archiver.llm")

#: 代码围栏匹配（模型爱把 JSON 包在 ```json 里）。
_FENCE_PATTERN = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)

#: 全角标点 → 半角。
_FULLWIDTH_TABLE = str.maketrans({"“": '"', "”": '"', "＂": '"', "：": ":", "，": ","})

#: 行注释 / 块注释 / 尾逗号。
_LINE_COMMENT_PATTERN = re.compile(r"(?<![:/\"'])//[^\n]*")
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
_TRAILING_COMMA_PATTERN = re.compile(r",\s*([}\]])")


@dataclass
class LLMCallResult:
    """一次模型调用的结果。

    Attributes:
        ok: 是否成功拿到文本。
        text: 返回的纯文本。
        model_tag: 实际使用的模型标识（写日志用）。
        error: 失败原因摘要。
    """

    ok: bool
    text: str = ""
    model_tag: str = ""
    error: str = ""


def _entry_identifier(entry: Any) -> str:
    """读取模型条目里的标识（写日志用）。"""
    if isinstance(entry, dict):
        for key in ("model_identifier", "name", "id"):
            value = entry.get(key)
            if value:
                return str(value)
    return ""


def resolve_model_set(
    config: ContextArchiverConfig,
    *,
    temperature: float | None = None,
    max_tokens: int | None = None,
) -> tuple[Any, str]:
    """按配置解析模型集，并按需覆盖温度与输出上限。

    Args:
        config: 插件配置。
        temperature: 覆盖温度；``None`` 表示用配置值。
        max_tokens: 覆盖输出上限；``None`` 表示用配置值。

    Returns:
        ``(model_set, model_tag)``。

    Raises:
        Exception: 模型解析失败时抛出底层异常，由调用方兜住。
    """
    target_temperature = (
        temperature if temperature is not None else float(config.model.temperature)
    )
    target_max_tokens = (
        max_tokens if max_tokens is not None else int(config.model.max_tokens)
    )

    model_name = str(config.model.model_name or "").strip()
    if model_name:
        model_set = llm_api.get_model_set_by_name(
            model_name,
            temperature=target_temperature,
            max_tokens=target_max_tokens,
        )
        return model_set, f"by_name:{model_name}"

    task_name = str(config.model.task_name or "").strip() or "actor"
    model_set = llm_api.get_model_set_by_task(task_name)

    overridden: list[Any] = []
    identifiers: list[str] = []
    for entry in model_set:
        if isinstance(entry, dict):
            patched = dict(entry)
            patched["temperature"] = target_temperature
            patched["max_tokens"] = target_max_tokens
            overridden.append(patched)
            identifier = _entry_identifier(entry)
            if identifier:
                identifiers.append(identifier)
        else:
            overridden.append(entry)

    tag = f"task:{task_name}"
    if identifiers:
        tag = f"{tag}:{identifiers[0]}"
    return overridden, tag


def extract_text(response: Any) -> str:
    """从 LLMResponse 中提取纯文本。

    Args:
        response: ``request.send`` 返回的响应对象。

    Returns:
        提取到的文本；无内容时返回空字符串。
    """
    message = getattr(response, "message", None)
    if message is None:
        return ""
    if isinstance(message, str):
        return message.strip()
    if isinstance(message, list):
        parts: list[str] = []
        for item in message:
            text = getattr(item, "text", None)
            if isinstance(text, str) and text:
                parts.append(text)
            elif isinstance(item, str):
                parts.append(item)
        return "".join(parts).strip()
    text = getattr(message, "text", None)
    if isinstance(text, str):
        return text.strip()
    return str(message).strip()


def extract_json(text: str) -> dict[str, Any] | None:
    """尽力从模型输出里抠出一个 JSON 对象。

    依次尝试：代码围栏 → 全角标点归一 → 去注释与尾逗号 → 最外层花括号切片。

    Args:
        text: 模型返回的文本。

    Returns:
        解析出的字典；失败返回 ``None``。
    """
    if not text or not text.strip():
        return None

    candidates: list[str] = []
    fenced = _FENCE_PATTERN.search(text)
    if fenced:
        candidates.append(fenced.group(1))

    translated = text.translate(_FULLWIDTH_TABLE)
    candidates.append(translated)

    start = translated.find("{")
    end = translated.rfind("}")
    if start >= 0 and end > start:
        candidates.append(translated[start : end + 1])

    for candidate in candidates:
        cleaned = _BLOCK_COMMENT_PATTERN.sub("", candidate)
        cleaned = _LINE_COMMENT_PATTERN.sub("", cleaned)
        cleaned = _TRAILING_COMMA_PATTERN.sub(r"\1", cleaned).strip()
        if not cleaned:
            continue
        try:
            parsed = json.loads(cleaned)
        except (ValueError, TypeError):
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


async def call(
    config: ContextArchiverConfig,
    *,
    system_prompt: str,
    user_prompt: str,
    temperature: float | None = None,
    max_tokens: int | None = None,
    request_name: str = "context_archiver",
) -> LLMCallResult:
    """执行一次单轮模型调用。

    Args:
        config: 插件配置。
        system_prompt: 系统提示词。
        user_prompt: 用户提示词。
        temperature: 覆盖温度。
        max_tokens: 覆盖输出上限。
        request_name: 请求名（写进 LLM 统计，便于排查）。

    Returns:
        调用结果。
    """
    try:
        model_set, model_tag = resolve_model_set(
            config,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    except Exception as error:  # noqa: BLE001 - 模型解析失败按调用失败处理
        logger.warning(f"[context_archiver] 解析模型集失败: {error}")
        return LLMCallResult(ok=False, error=f"resolve model set: {error}")

    try:
        request = llm_api.create_llm_request(model_set, request_name=request_name)
        request.add_payload(LLMPayload(ROLE.SYSTEM, Text(system_prompt)))
        request.add_payload(LLMPayload(ROLE.USER, Text(user_prompt)))
        response = await request.send(stream=False)
        await response
    except Exception as error:  # noqa: BLE001 - 调用失败按失败结果返回
        logger.warning(f"[context_archiver] 模型调用失败: {error}")
        return LLMCallResult(ok=False, model_tag=model_tag, error=str(error))

    text = extract_text(response)
    if not text:
        return LLMCallResult(ok=False, model_tag=model_tag, error="empty response")
    return LLMCallResult(ok=True, text=text, model_tag=model_tag)


__all__ = [
    "LLMCallResult",
    "call",
    "extract_json",
    "extract_text",
    "resolve_model_set",
]
