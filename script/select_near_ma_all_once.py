"""全市场成本均线选股：启动后立刻跑一轮，跑完结束。

逻辑与 ``select_near_ma_all_xtquant.py`` 完全相同（沪深京A股、打分、入库），
只是不做 16:00 调度循环。适合手动验证或临时补跑当天结果。

日常定时请用 ``select_near_ma_all_xtquant.py``。
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

import select_near_ma_all_xtquant as all_ma

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


def run(engine: ScriptEngine) -> None:
    """ScriptTrader 入口：立即执行一轮全市场选股后退出。"""
    engine.write_log(
        f"全市场选股立即执行一轮，标的池={all_ma.SECTOR_NAME}，跑完即结束"
    )
    try:
        all_ma._run_once(engine)  # noqa: SLF001 - 复用全市场单轮逻辑
    except Exception:  # noqa: BLE001 - 异常打日志后仍正常结束脚本
        engine.write_log(f"本轮执行异常：\n{traceback.format_exc()}")
    engine.write_log("全市场选股本轮结束")
