# [Ref: 09_核心模块架构规约] [Ref: 11_数据采集与输入层规约] Module B 量化扫描引擎
# 标的池由 get_current_a_share_universe() 或调用方传入；对全部 N 只全量扫描，不写死数量

from diting.scanner.quant import QuantScanner

__all__ = ["QuantScanner"]
