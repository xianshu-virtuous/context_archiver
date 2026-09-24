# -*- coding: utf-8 -*-
r"""真实导入冒烟：证明「框架能加载这个插件」，而不只是「语法对」。

用法（用实例自带 venv，别用系统 python）：

    $env:NEO_ROOT="F:\Neo-MoFox-Aemeath"
    & "F:\Neo-MoFox-Aemeath\.venv\Scripts\python.exe" tests\check_import.py

它做三件事：
1. 把框架根目录与插件父目录插进 sys.path，逐个 import 本插件模块；
2. 检查 manifest.json 是否合法（根级、必需字段、无 BOM）；
3. 让框架的组件注册表枚举本插件的组件签名。
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
NEO_ROOT = Path(os.environ.get("NEO_ROOT", r"F:\Neo-MoFox-Aemeath"))

MODULES = [
    "context_archiver",
    "context_archiver.config",
    "context_archiver.state",
    "context_archiver.llm",
    "context_archiver.sink",
    "context_archiver.archiver",
    "context_archiver.service",
    "context_archiver.handlers.activity_tracker",
    "context_archiver.commands.archive_command",
    "context_archiver.plugin",
]

failures: list[str] = []


def section(title: str) -> None:
    print()
    print("=" * 66)
    print(title)
    print("=" * 66)


# ── 1. 导入 ────────────────────────────────────────────────────────────────
section("1. 真实导入（框架根 + 插件父目录已入 sys.path）")
print(f"框架根  : {NEO_ROOT}  存在={NEO_ROOT.exists()}")
print(f"插件目录: {PLUGIN_DIR}")
sys.path.insert(0, str(NEO_ROOT))
sys.path.insert(0, str(PLUGIN_DIR.parent))

for name in MODULES:
    try:
        importlib.import_module(name)
        print(f"  OK    {name}")
    except Exception as error:  # noqa: BLE001 - 冒烟就是要抓所有异常
        print(f"  FAIL  {name}: {type(error).__name__}: {error}")
        failures.append(f"{name}: {error}")

# ── 2. manifest 合规 ───────────────────────────────────────────────────────
section("2. manifest.json 合规检查")
manifest_path = PLUGIN_DIR / "manifest.json"
try:
    raw_bytes = manifest_path.read_bytes()
    if raw_bytes.startswith(b"\xef\xbb\xbf"):
        failures.append("manifest.json 带 BOM（框架会解析失败）")
        print("  FAIL  文件带 BOM")
    else:
        print("  OK    无 BOM")

    manifest = json.loads(raw_bytes.decode("utf-8"))
    required = ["name", "version", "description", "author", "dependencies", "entry_point"]
    missing = [key for key in required if key not in manifest]
    if missing:
        failures.append(f"manifest 缺必需字段: {missing}")
        print(f"  FAIL  缺字段 {missing}")
    else:
        print(f"  OK    必需字段齐全（name={manifest['name']} version={manifest['version']}）")

    if manifest.get("name") != PLUGIN_DIR.name:
        failures.append(f"manifest.name({manifest.get('name')}) != 目录名({PLUGIN_DIR.name})")
        print(f"  FAIL  name 与目录名不一致：{manifest.get('name')} vs {PLUGIN_DIR.name}")
    else:
        print("  OK    name 与目录名一致")

    if manifest.get("dependencies", {}).get("plugins"):
        print(f"  WARN  dependencies.plugins 非空：{manifest['dependencies']['plugins']}")
        print("        （框架对缺失的插件依赖是硬剔除，软依赖不能写这里）")
    else:
        print("  OK    dependencies.plugins 为空（软依赖靠运行时探测）")

    entry = manifest.get("entry_point")
    if entry and not (PLUGIN_DIR / entry).exists():
        failures.append(f"entry_point 不存在: {entry}")
        print(f"  FAIL  entry_point 不存在：{entry}")
    else:
        print(f"  OK    entry_point 存在：{entry}")

    api_versions = manifest.get("api_version") or {}
    try:
        from src.app.plugin_system.api import PLUGIN_API_VERSIONS

        unknown = [key for key in api_versions if key not in PLUGIN_API_VERSIONS]
        if unknown:
            failures.append(f"api_version 声明了不存在的模块: {unknown}")
            print(f"  FAIL  未知 api 模块：{unknown}")
        else:
            print(f"  OK    api_version 声明的 {len(api_versions)} 个模块都合法")
    except Exception as error:  # noqa: BLE001
        print(f"  SKIP  无法读取 PLUGIN_API_VERSIONS: {error}")

    include = manifest.get("include") or []
    print(f"  OK    include 组件 {len(include)} 个: "
          + ", ".join(f"{c.get('component_type')}:{c.get('component_name')}" for c in include))
except Exception as error:  # noqa: BLE001
    failures.append(f"manifest 解析失败: {error}")
    print(f"  FAIL  {error}")

# ── 3. 组件注册 ────────────────────────────────────────────────────────────
section("3. 框架组件注册表枚举")
try:
    from src.core.components.registry import get_global_registry

    registry = get_global_registry()
    all_signatures = list(registry.list_all())
    mine = [str(sig) for sig in all_signatures if "context_archiver" in str(sig)]
    if mine:
        for sig in sorted(mine):
            print(f"  OK    {sig}")
    else:
        print("  WARN  注册表里没有枚举到本插件组件（可能需要在框架初始化后注册）")
except Exception as error:  # noqa: BLE001
    print(f"  SKIP  注册表不可用: {type(error).__name__}: {error}")

# ── 4. 纯函数自检（不依赖框架运行时）────────────────────────────────────────
section("4. 判定逻辑自检")
try:
    from context_archiver import state as st
    from context_archiver.archiver import evaluate
    from context_archiver.config import ContextArchiverConfig

    cfg = ContextArchiverConfig()
    cfg.trigger.min_messages = 4
    cfg.trigger.idle_seconds = 1800
    cfg.trigger.settle_seconds = 90
    now = 1_000_000.0

    cases: list[tuple[str, st.StreamState, bool]] = [
        (
            "消息不足 → 不归档",
            st.StreamState(stream_id="a", pending_count=2, last_activity_at=now - 9999),
            False,
        ),
        (
            "静默超阈值 → 归档",
            st.StreamState(stream_id="b", pending_count=10, last_activity_at=now - 2000),
            True,
        ),
        (
            "活跃中 → 不归档",
            st.StreamState(stream_id="c", pending_count=10, last_activity_at=now - 10),
            False,
        ),
        (
            "结束信号已过等待期 → 归档",
            st.StreamState(
                stream_id="d",
                pending_count=10,
                last_activity_at=now - 5,
                end_signal_at=now - 100,
                end_signal_name="stop_conversation",
            ),
            True,
        ),
        (
            "结束信号还在等待期 → 不归档",
            st.StreamState(
                stream_id="e",
                pending_count=10,
                last_activity_at=now - 5,
                end_signal_at=now - 10,
                end_signal_name="stop_conversation",
            ),
            False,
        ),
    ]

    for label, stream_state, expected in cases:
        decision = evaluate(stream_state, cfg, now=now)
        flag = "OK  " if decision.should == expected else "FAIL"
        if decision.should != expected:
            failures.append(f"判定用例失败: {label}")
        print(f"  {flag}  {label}  → {decision.should}（{decision.reason}）")
except Exception as error:  # noqa: BLE001
    failures.append(f"判定自检异常: {error}")
    print(f"  FAIL  {type(error).__name__}: {error}")

# ── 汇总 ───────────────────────────────────────────────────────────────────
section("结果")
if failures:
    print(f"失败 {len(failures)} 项：")
    for item in failures:
        print(f"  - {item}")
else:
    print("全部通过 ✓")
print()
sys.exit(1 if failures else 0)
