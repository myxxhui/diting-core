"""A-track position lifecycle helpers: stop tightening and signal review streaks.

[Ref: 03_A轨_持仓与每日信号复核规约] (diting-doc Stage3)
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional, Tuple

Side = Literal["long", "short"]


def merge_stop_tighten_only(
    current_stop: Optional[float],
    suggested_stop: float,
    side: Side,
) -> float:
    """Merge suggested stop with existing stop; default policy is tighten-only.

    Long: tighter = higher stop price (closer to entry from below) -> max.
    Short: tighter = lower stop price -> min.
    """
    if current_stop is None:
        return suggested_stop
    if side == "long":
        return max(current_stop, suggested_stop)
    return min(current_stop, suggested_stop)


def update_exit_streak(
    current_streak: int,
    latest_score: Optional[float],
    min_score: float,
    required_consecutive: int,
    *,
    missing_as_below: bool = True,
) -> Tuple[int, bool]:
    """Update consecutive-below streak; return (new_streak, should_exit).

    If latest_score is None and missing_as_below is True, counts as a failing day.
    If None and missing_as_below is False, streak unchanged (no pass/fail).
    """
    if latest_score is None:
        if not missing_as_below:
            return (current_streak, False)
        new_streak = current_streak + 1
        return (new_streak, new_streak >= required_consecutive)
    if latest_score >= min_score:
        return (0, False)
    new_streak = current_streak + 1
    return (new_streak, new_streak >= required_consecutive)


def daily_b_resets_tp_ladder_default() -> bool:
    """Policy default: do not replace TP ladder from each daily B snapshot."""
    return False


def load_position_lifecycle_config(path: Optional[Path] = None) -> dict:
    """Load YAML config; defaults to diting-core/config/a_track_position_lifecycle.yaml."""
    import yaml

    base = Path(__file__).resolve().parents[1]
    p = path or (base / "config" / "a_track_position_lifecycle.yaml")
    with p.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
