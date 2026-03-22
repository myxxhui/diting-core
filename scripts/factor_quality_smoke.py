#!/usr/bin/env python3
# [Ref: 02_B模块策略_策略实现规约] Alphalens 因子分层质检门禁；无依赖时 exit 0（CI 友好）
# 建议：pip install -r requirements-scanner-alphalens.txt（或 make deps-scanner-alphalens）；勿装旧包 alphalens 0.4.0。

from __future__ import annotations

import sys


def main() -> int:
    try:
        import alphalens as al
    except ImportError:
        print("factor_quality_smoke: alphalens 未安装，跳过质检门禁（exit 0）")
        return 0
    try:
        import pandas as pd
        import numpy as np
    except ImportError:
        print("factor_quality_smoke: pandas/numpy 未安装，跳过（exit 0）")
        return 0

    # 合成 5×20 日面板：因子 + 前向收益；验证 get_clean_factor_and_forward_returns 可跑通
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=20, freq="B")
    symbols = [f"S{i:04d}" for i in range(5)]
    idx = pd.MultiIndex.from_product([dates, symbols], names=["date", "asset"])
    factor = pd.Series(rng.standard_normal(len(idx)), index=idx)
    prices = pd.DataFrame(
        rng.lognormal(0.0, 0.01, size=(len(dates), len(symbols))).cumsum(axis=0) + 10.0,
        index=dates,
        columns=symbols,
    )
    try:
        out = al.utils.get_clean_factor_and_forward_returns(
            factor, prices, quantiles=5, periods=(1, 5)
        )
    except Exception as e:
        print("factor_quality_smoke: Alphalens 管线异常: %s（exit 1）" % e)
        return 1
    # alphalens-reloaded 仅返回 merged DataFrame；旧 alphalens 曾返回 (factor_data, forward_returns)
    factor_data = out[0] if isinstance(out, tuple) else out
    if factor_data is None or len(factor_data) < 1:
        print("factor_quality_smoke: 空结果（exit 1）")
        return 1
    print(
        "factor_quality_smoke: Alphalens 分层管线 OK；clean_factor 行数=%s（exit 0）"
        % len(factor_data)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
