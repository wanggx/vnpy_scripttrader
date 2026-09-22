"""全市场止跌形态扫描：启动后立刻跑一轮，跑完结束。

逻辑与 ``select_halt_downturn_xtquant.py`` 完全相同（沪深A股、严格档小十字星+
缩量、打分、入库），只是不做 16:30 调度循环。适合手动验证或临时补跑当天结果。

与定时版的区别：定时版排在 16:00 的 ``select_near_ma_*`` 之后，默认直接读本地
数据不补数；这里手动跑没有前置补数，所以**强制开启补数**（可能较慢）。

日常定时请用 ``select_halt_downturn_xtquant.py``。
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

import select_halt_downturn_xtquant as halt

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


def run(engine: ScriptEngine) -> None:
    """ScriptTrader 入口：立即执行一轮全市场止跌形态扫描后退出。"""
    engine.write_log(
        "全市场止跌形态扫描立即执行一轮（小十字星+极致缩量），跑完即结束"
    )
    try:
        # 手动/补跑场景没有前置补数，显式打开补数开关（定时版默认不补数）。
        halt._run_once(engine, download_missing=True)  # noqa: SLF001 - 复用主脚本单轮逻辑
    except Exception:  # noqa: BLE001 - 异常打日志后仍正常结束脚本
        engine.write_log(f"本轮执行异常：\n{traceback.format_exc()}")
    engine.write_log("全市场止跌形态扫描本轮结束")
