"""为成本均线选股脚本补全全市场 A 股日线（经 BigQMT RPC）。

select_near_ma_xtquant.py / select_near_ma_all_xtquant.py 的成本均线需从最早
固定日期起算。优先在大 QMT「数据管理」补历史；本脚本经 RPC 尽力
``download_history_data2``（大 QMT 上该接口常不可用）。

下载起点直接取选股脚本 ``FIXED_DATES`` 的最早值。
"""

from typing import TYPE_CHECKING

import download_xtquant_daily
import select_near_ma_xtquant

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


START_DATE: str = min(select_near_ma_xtquant.FIXED_DATES)


def run(engine: "ScriptEngine") -> None:
    """ScriptTrader 策略入口：从 START_DATE 起补全全市场日线。

    一次性历史回填，传 force_full=True 跳过「最新根已存在即跳过」的增量
    判定，否则偶然下过最新一天的标的会被漏掉更早的历史缺口。
    """
    download_xtquant_daily.run(engine, start_date=START_DATE, force_full=True)
