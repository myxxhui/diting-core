# [Ref: 待办_全A股标的池_20260301.md T2] get_current_a_share_universe 单测
# 验收：mock 表/数据下返回预期列表；检查→不更新→返回、检查→更新→返回 两条路径可测

from datetime import datetime, timezone
from unittest.mock import MagicMock

from diting.universe import (
    _is_valid_updated_at,
    get_current_a_share_universe,
)


def test_is_valid_updated_at_today():
    """当日 updated_at 视为有效。"""
    now = datetime.now(timezone.utc)
    assert _is_valid_updated_at(now) is True


def test_is_valid_updated_at_none():
    """None 视为无效。"""
    assert _is_valid_updated_at(None) is False


def test_is_valid_updated_at_old():
    """昨日及更早视为无效。"""
    from datetime import timedelta
    old = datetime.now(timezone.utc) - timedelta(days=1)
    assert _is_valid_updated_at(old) is False


def test_get_current_a_share_universe_valid_no_refresh():
    """有效数据：不调用 refresh，直接返回表内列表（检查→不更新→返回）。"""
    now = datetime.now(timezone.utc)
    expected = ["000001.SZ", "600000.SH"]
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = (now,)
    mock_cur.fetchall.return_value = [(s,) for s in expected]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    refresh_called = []

    def track_refresh():
        refresh_called.append(1)

    result = get_current_a_share_universe(conn=mock_conn, refresh_callback=track_refresh)
    assert result == expected
    assert len(refresh_called) == 0


def test_get_current_a_share_universe_invalid_then_refresh():
    """无效数据：调用 refresh_callback 后再读表返回（检查→更新→返回）。"""
    expected = ["000001.SZ", "600000.SH"]
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = (None,)  # 第一次 SELECT MAX 返回 None（无效）
    mock_cur.fetchall.return_value = [(s,) for s in expected]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    refresh_called = []

    def track_refresh():
        refresh_called.append(1)

    result = get_current_a_share_universe(conn=mock_conn, refresh_callback=track_refresh)
    assert result == expected
    assert len(refresh_called) == 1


def test_get_current_a_share_universe_force_refresh():
    """force_refresh=True 时先刷新再读表。"""
    expected = ["A.SZ", "B.SH"]
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = [(s,) for s in expected]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    refresh_called = []

    def track_refresh():
        refresh_called.append(1)

    result = get_current_a_share_universe(
        conn=mock_conn, refresh_callback=track_refresh, force_refresh=True
    )
    assert result == expected
    assert len(refresh_called) == 1
