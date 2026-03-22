# [Ref: 02_B模块策略_策略实现规约 §3.4]

from diting.scanner.quant import _sector_strength_ratios


def test_sector_strength_disabled_all_ones():
    ss, mp = _sector_strength_ratios([80.0, 40.0], ["A", "B"], {}, False)
    assert ss == [1.0, 1.0]
    assert mp == [None, None]


def test_sector_strength_no_industry_uses_unmapped_default():
    ss, mp = _sector_strength_ratios([80.0, 40.0], ["A", "B"], {}, True)
    assert ss == [1.0, 1.0]
    assert mp == [False, False]


def test_sector_strength_no_industry_custom_unmapped():
    ss, mp = _sector_strength_ratios(
        [80.0, 40.0], ["A", "B"], {}, True, unmapped_sector_strength=0.95
    )
    assert ss == [0.95, 0.95]
    assert mp == [False, False]


def test_sector_strength_same_industry_ratio():
    ind = {"S1.SZ": "银行", "S2.SZ": "银行"}
    ts = [80.0, 40.0]
    ss, mp = _sector_strength_ratios(ts, ["S1.SZ", "S2.SZ"], ind, True)
    mean = 60.0
    assert abs(ss[0] - 80.0 / mean) < 1e-9
    assert abs(ss[1] - 40.0 / mean) < 1e-9
    assert mp == [True, True]
