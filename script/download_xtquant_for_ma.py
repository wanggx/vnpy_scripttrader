"""为成本均线选股脚本一次性下载全市场 A 股全量日线。

select_near_ma_xtquant.py / select_near_ma_all_xtquant.py 的成本均线需从最早
固定日期起算。首次使用前请运行本脚本拉取全量历史；之后按需重跑本脚本或
download_xtquant_daily.py 补缺口即可（incrementally，已有的不重下）。

下载起点直接取选股脚本 ``FIXED_DATES`` 的最早值，避免两处硬编码不同步导致
均线窗口数据缺失。实际下载逻辑见 ``download_xtquant_daily.py``：按标的看
本地最后一根，缺的一次传入全部代码拉区间，不再按天扫描、不再 500 一批循环。
"""

from typing import TYPE_CHECKING

import download_xtquant_daily
import select_near_ma_xtquant

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


# 下载起始日期：直接取选股脚本 FIXED_DATES 的最早值，保证下载区间与选股
# 均线窗口起点一致；改 FIXED_DATES 即自动同步，无需手动维护本常量。
START_DATE: str = min(select_near_ma_xtquant.FIXED_DATES)


def run(engine: "ScriptEngine") -> None:
    """ScriptTrader 策略入口：从 START_DATE 起补全全市场日线。"""
    download_xtquant_daily.run(engine, start_date=START_DATE)
