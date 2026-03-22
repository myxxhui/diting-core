"""Tests for diting.position_lifecycle."""

from diting.position_lifecycle import (
    daily_b_resets_tp_ladder_default,
    merge_stop_tighten_only,
    update_exit_streak,
)


def test_merge_stop_long_tighten_only():
    assert merge_stop_tighten_only(95.0, 96.0, "long") == 96.0
    assert merge_stop_tighten_only(95.0, 94.0, "long") == 95.0
    assert merge_stop_tighten_only(None, 94.0, "long") == 94.0


def test_merge_stop_short_tighten_only():
    assert merge_stop_tighten_only(105.0, 104.0, "short") == 104.0
    assert merge_stop_tighten_only(105.0, 106.0, "short") == 105.0


def test_update_exit_streak():
    assert update_exit_streak(0, 50.0, 60, 3) == (1, False)
    assert update_exit_streak(1, 55.0, 60, 3) == (2, False)
    assert update_exit_streak(2, 40.0, 60, 3) == (3, True)
    assert update_exit_streak(2, 70.0, 60, 3) == (0, False)


def test_update_exit_streak_missing():
    assert update_exit_streak(0, None, 60, 2, missing_as_below=True) == (1, False)
    assert update_exit_streak(1, None, 60, 2, missing_as_below=True) == (2, True)
    assert update_exit_streak(1, None, 60, 2, missing_as_below=False) == (1, False)


def test_daily_tp_default():
    assert daily_b_resets_tp_ladder_default() is False
