# [Ref: 03_双轨制与VC-Agent, 04_B轨_C模块右脑与信号设计] 轨标识，贯穿流水线
# 各模块据此选择配置与行为（信号层适配器、C 输出 horizon 等）

from enum import Enum
from typing import Optional


class Track(str, Enum):
    """轨：A=短线 SHORT_TERM，B=中线 MEDIUM_TERM"""
    A = "a"
    B = "b"


def parse_track(raw: Optional[str]) -> Track:
    """从 ENV 或参数解析 track；非法则默认 A。"""
    if not raw or not str(raw).strip():
        return Track.A
    r = str(raw).strip().lower()
    if r in ("b", "medium", "mid"):
        return Track.B
    return Track.A


def get_track_from_env() -> Track:
    """读取 DITING_TRACK 环境变量。"""
    return parse_track(__import__("os").environ.get("DITING_TRACK"))
