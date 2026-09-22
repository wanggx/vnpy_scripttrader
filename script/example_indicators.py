"""示例：调用通用指标模块给一批标的算 MACD / KDJ / RSI / BOLL / ATR。

演示「通用脚本 + 调用方」的标准三步（新写选股/信号脚本照抄这三步即可）：

1. ``market_data.load_ohlcv_series`` 统一取数（探本地覆盖 → 只补缺的标的 → 分批读全区间）；
2. ``indicators.compute_many`` 一次算出多条指标；
3. ``indicators.latest`` 取 T 日快照 + ``indicators.cross_over`` 判金叉，写选股条件。

只打印结果、不写数据库，适合先验证口径再落库。行情走大 QMT RPC，
需大 QMT 端 BIGQMT 服务端策略与 RPC 桥就绪后经 ScriptTrader 执行。
"""

from __future__ import annotations

# pylint: disable=protected-access
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import bigqmt_xtdata

# 通用模块目录：script/talib（指标计算）、script/market（行情取数）。两个目录都不放
# __init__.py（talib 有 __init__.py 会遮蔽真正的 TA-Lib），目录加入 sys.path 后直接 import。
_TALIB_DIR = Path(__file__).resolve().parent / "talib"
_MARKET_DIR = Path(__file__).resolve().parent / "market"
for _dir in (_TALIB_DIR, _MARKET_DIR):
    if str(_dir) not in sys.path:
        sys.path.insert(0, str(_dir))

import indicators as ind  # noqa: E402
import market_data as md  # noqa: E402

if TYPE_CHECKING:
    import pandas as pd

    from vnpy_scripttrader.engine import ScriptEngine


# 本次要算的指标（name -> 参数覆盖），键名见 indicators.available()。
SPECS: dict[str, dict[str, Any]] = {
    "macd": {},
    "kdj": {},
    "rsi": {"n": 6},
    "boll": {},
    "atr": {},
}


def _describe(code: str, name: str, bars: "pd.DataFrame", snapshot: dict[str, float]) -> str:
    """把单标的的 T 日指标快照拼成一行日志文本。"""

    def fmt(key: str) -> str:
        value = snapshot.get(key)
        return "nan" if value is None else f"{value:.2f}"

    return (
        f"{code} {name} 收={float(bars['close'].iloc[-1]):.2f} "
        f"DIF={fmt('DIF')} DEA={fmt('DEA')} MACD={fmt('MACD')} "
        f"K={fmt('K')} D={fmt('D')} J={fmt('J')} "
        f"RSI6={fmt('RSI6')} BOLL上轨={fmt('BOLL_UPPER')} ATR14={fmt('ATR14')}"
    )


def run(
    engine: ScriptEngine,
    sample_size: int = 20,
    lookback_days: int = 400,
) -> None:
    """对全市场里前 sample_size 只标的算一遍指标并打印。

    参数说明：
      sample_size    取样标的数（示例用，越小越快）
      lookback_days  读取的日历日回看天数（默认 400 天，够 120+ 根日线）
    """
    engine.write_log(
        f"指标示例启动：取样 {sample_size} 只，回看 {lookback_days} 天，"
        f"指标={ind.available()}"
    )
    try:
        if not bigqmt_xtdata.ping(engine):
            engine.write_log("大 QMT RPC 不可用，示例结束")
            return

        # ---- 步骤 1：取数（标的池 + 区间）----
        codes: list[str] = md.get_all_stock_codes(engine)[:sample_size]
        if not codes:
            engine.write_log("大 QMT 未返回任何 A 股代码，示例结束")
            return
        universe: list[tuple[str, str]] | None = md.filter_universe(engine, codes)
        if universe is None:
            engine.write_log("示例已停止（筛选阶段）")
            return
        if not universe:
            engine.write_log("筛选后无可用标的，示例结束")
            return

        end_date: str = datetime.now().strftime("%Y%m%d")
        start_date: str = (datetime.now() - timedelta(days=lookback_days)).strftime(
            "%Y%m%d"
        )
        calendar: list[str] = md.get_trading_dates(start_date, end_date)
        series: dict[str, pd.DataFrame] | None = md.load_ohlcv_series(
            engine, universe, start_date, end_date
        )
        if series is None:
            engine.write_log("示例已停止（读取数据阶段）")
            return
        if not series:
            engine.write_log("未读到任何行情数据，请先在大 QMT「数据管理」补全日线")
            return

        trade_date: str = md.decide_trade_date(engine, series, calendar)
        engine.write_log(f"本次计算交易日 T = {trade_date}，读到 {len(series)} 只标的")

        # ---- 步骤 2 + 3：算指标、取快照、判信号 ----
        name_map: dict[str, str] = dict(universe)
        golden: int = 0
        oversold: int = 0
        for code, bars in series.items():
            if not engine.is_active():
                engine.write_log("示例已停止（计算阶段）")
                return
            bars = bars.loc[:trade_date]
            if bars.empty:
                continue

            frame = ind.compute_many(bars, SPECS)
            snapshot = ind.latest(bars, SPECS)
            engine.write_log(_describe(code, name_map.get(code, ""), bars, snapshot))

            # 金叉：DIF 上穿 DEA（用整段序列判断，取 T 日布尔值）。
            if bool(ind.cross_over(frame["DIF"], frame["DEA"]).iloc[-1]):
                golden += 1
            if snapshot.get("RSI6", 100.0) < 30.0:
                oversold += 1

        engine.write_log(
            f"完成：DIF 上穿 DEA {golden} 只，RSI6<30（超卖）{oversold} 只"
        )
    except Exception:  # noqa: BLE001 - 示例脚本异常打日志后结束
        engine.write_log(f"示例执行异常：\n{traceback.format_exc()}")
