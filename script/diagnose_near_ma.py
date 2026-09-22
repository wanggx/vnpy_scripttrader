"""诊断单只标的在某个交易日为何未被 select_near_ma 系列脚本选中。

用法（在 script/ 目录下，或把该目录加入 PYTHONPATH）：
    python diagnose_near_ma.py 002218.SZ 20260911

逐条打印 ``select_near_ma_xtquant`` 里的过滤条件：ST、板块成分、数据条数、
腰斩过滤、每条成本均线的接近/跌穿判定，最后给出会得到的分数。
"""

from __future__ import annotations

import sys

import pandas as pd

import bigqmt_xtdata
from bigqmt_xtdata import instrument_name, xtdata
import select_near_ma_xtquant as ma
from select_near_ma_all_xtquant import PRIMARY_SECTOR


def main() -> None:
    """打印单只标的在每个过滤环节的判定结果。"""
    code: str = sys.argv[1] if len(sys.argv) > 1 else "002218.SZ"
    trade_date: str = sys.argv[2] if len(sys.argv) > 2 else "20260911"

    print(f"=== 诊断 {code} @ T={trade_date} ===")

    # 1. 合约信息 / ST
    detail = xtdata.get_instrument_detail(code) or {}
    name: str = instrument_name(detail)
    print(f"[1] 合约名称={name!r} detail_keys={len(detail)}")
    if not name and not detail:
        print("    → 合约信息缺失，脚本会直接剔除")
        return
    if ma.EXCLUDE_ST and "ST" in name.upper():
        print("    → 名称含 ST，被 EXCLUDE_ST 剔除")
        return

    # 2. 板块成分
    codes = xtdata.get_stock_list_in_sector(PRIMARY_SECTOR) or []
    print(f"[2] 板块“{PRIMARY_SECTOR}”成分数={len(codes)} 含该标的={code in codes}")

    # 3. 交易日历与窗口起点
    end_date = trade_date
    calendar = ma._get_trading_dates(ma.FIXED_DATES[0], end_date)
    window_start = {
        d: min(x for x in calendar if x >= d) for d in ma.FIXED_DATES
    }
    start_min = min(window_start.values())
    print(f"[3] window_start={window_start} start_min={start_min}")

    # 4. 读行情（与选股同一接口/参数）
    data = xtdata.get_market_data_ex(
        field_list=["close", "high", "low"],
        stock_list=[code],
        period="1d",
        start_time=start_min,
        end_time=end_date,
        count=-1,
        dividend_type=ma.DIVIDEND_TYPE,
        fill_data=False,
    )
    df = data.get(code)
    if df is None or len(df) == 0:
        print("    → 未读到行情数据（终端本地库缺日线）")
        return
    bars = ma._normalize_bars(df)
    close, high, low = bars["close"], bars["high"], bars["low"]
    valid_all = close.dropna()
    print(
        f"[4] 有效交易日={valid_all.size}（MIN_BARS={ma.MIN_BARS}）"
        f" 首日={valid_all.index[0]} 末日={valid_all.index[-1]}"
    )
    if valid_all.size < ma.MIN_BARS:
        print("    → 数据不足 MIN_BARS，被剔除")
        return
    print(f"    最近5根：\n{bars.tail(5)}")
    print(f"    index dtype={bars.index.dtype}")

    if trade_date not in close.index:
        print(f"    → T={trade_date} 不在该标的日线索引中，_score_symbol 返回 None")
        return
    close_t = float(close.loc[trade_date])
    low_t = float(low.loc[trade_date]) if trade_date in low.index else float("nan")
    print(f"[5] T 日 close={close_t} low={low_t}")

    # 6. 腰斩过滤
    high_max = ma._recent_high(close, high, trade_date)
    halved = ma._is_halved(close, high, trade_date)
    print(
        f"[6] 近{ma.HIGH_LOOKBACK}日最高={high_max} 腰斩线={high_max * ma.HALF_RATIO} "
        f"要求 close<={high_max * ma.HALF_RATIO} → halved={halved} "
        f"(REQUIRE_HALVED={ma.REQUIRE_HALVED})"
    )
    if ma.REQUIRE_HALVED and not halved:
        print("    → 未腰斩，被硬过滤剔除（不可能入选）。以下继续分析均线，仅供参考：")

    # 7. 逐条均线判定
    listed = str(valid_all.index[0])
    print(f"[7] 上市首根={listed}，逐条均线：")
    near_weight = 0.0
    valid_weight_total = 0.0
    near_hits: list[str] = []
    for fixed_date in ma.FIXED_DATES:
        start = window_start[fixed_date]
        if listed > start:
            print(f"    {fixed_date}: 起点 {start} 早于上市首日 → 该均线无效")
            continue
        valid = close.loc[start:trade_date].dropna()
        n = valid.size
        if n < ma.MIN_WINDOW_POINTS:
            print(f"    {fixed_date}: 有效点 {n} < {ma.MIN_WINDOW_POINTS} → 无效")
            continue
        mavg = float(valid.mean())
        weight = ma._weight(n)
        valid_weight_total += weight
        diff = abs(close_t - mavg) / mavg if mavg > 0 else float("inf")
        diff_low = abs(low_t - mavg) / mavg if mavg > 0 else float("inf")
        broke = ma._broke_below_ma(valid, trade_date)
        # 收盘价、最低价任一落在带内即算靠近。
        is_near = mavg > 0 and (
            diff <= ma.NEAR_THRESHOLD or diff_low <= ma.NEAR_THRESHOLD
        )
        if is_near and not broke:
            near_hits.append(fixed_date)
            near_weight += weight
        print(
            f"    {fixed_date}: start={start} n={n} ma={mavg:.4f} "
            f"偏离(收)={diff * 100:.2f}% 偏离(低)={diff_low * 100:.2f}% "
            f"near={is_near} 近期跌穿={broke} 权重={weight}"
        )

    score = round(near_weight / valid_weight_total * 100) if valid_weight_total > 0 else 0
    print(
        f"[8] 命中均线={near_hits} score={score} "
        f"(valid_weight_total={valid_weight_total})"
    )
    if not near_hits:
        print("    → 未接近任何有效均线，未入选")
    print("[9] 提示：入选还需 score>0 且进入 TOP_N 排名")


if __name__ == "__main__":
    main()
