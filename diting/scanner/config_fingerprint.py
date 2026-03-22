# [Ref: 02_B模块策略_策略实现规约] scanner_rules.yaml 内容指纹，用于 L2 行与 metrics 追溯「本批按哪版配置产出」

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

# 与 config_loader._DEFAULT_CONFIG_PATH 一致
_DEFAULT_RULES = Path(__file__).resolve().parents[2] / "config" / "scanner_rules.yaml"


def compute_scanner_rules_fingerprint(config_path: Optional[Path] = None) -> str:
    """
    对 scanner_rules.yaml **原始字节**做 SHA-256，取十六进制前 16 位（短指纹，便于终端与索引展示）。
    文件不存在时对固定占位串哈希，避免空指纹。
    """
    path = Path(config_path) if config_path else _DEFAULT_RULES
    if path.exists():
        data = path.read_bytes()
    else:
        data = b"scanner_rules.yaml_missing"
    return hashlib.sha256(data).hexdigest()[:16]
