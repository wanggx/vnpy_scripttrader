"""列出单只标的在一段日期内每个交易日是否满足入选条件。

用法（在 script/ 目录下）：
    python diagnose_near_ma_range.py 002218.SZ 20260801 20260911

输出每日的：收盘、是否腰斩、命中的均线、分数、最接近的一条均线的偏离度。
用于复盘"某天为什么选不上 / 哪天本来能选上"。
"""

from __future__ import annotations

import sys
from datetime import datetime

from bigqmt_xtdata import xtdata
import select_near_ma_xtquant as ma


def main() -> None:
    """按交易日逐日打印入选判定。"""
    code: str = sys.argv[1] if len(sys.argv) > 1 else "002218.SZ"
    start: str = sys.argv[2] if len(sys.argv) > 2 else "20260601"
    end: str = sys.argv[3] if len(sys.argv) > 3 else datetime.now().strftime("%Y%m%d")
    only_hits: bool = "--hits" in sys.argv

    # 不调用 xtdata.get_trading_dates（大 QMT 上该接口偶发长时间无响应）：
    # 用标的自身日线索引当交易日序列，窗口起点取该标的首个 >= 固定日期的交易日。
    data = xtdata.get_market_data_ex(
        field_list=["close", "high", "low"],
        stock_list=[code],
        period="1d",
        start_time=ma.FIXED_DATES[0],
        end_time=end,
        count=-1,
        dividend_type=ma.DIVIDEND_TYPE,
        fill_data=False,
    )
    raw = data.get(code)
    if raw is None or len(raw) == 0:
        print(f"{code}: 未读到行情数据")
        return
    bars = ma._normalize_bars(raw)
    close, high, low = bars["close"], bars["high"], bars["low"]
    index = list(close.index)

    window_start: dict[str, str] = {}
    for fixed_date in ma.FIXED_DATES:
        nxt = next((d for d in index if d >= fixed_date), None)
        if nxt is None:
            print(f"警告：无 {fixed_date} 之后的日线，该均线无法计算")
            continue
        window_start[fixed_date] = nxt

    print(f"{code} 逐日判定（{start} ~ {end}，阈值 {ma.NEAR_THRESHOLD:.0%}）：")
    print(f"{'日期':<10}{'收盘':>7}{'腰斩':>6}{'命中均线':>18}{'分数':>6}{'偏离(收/低)':>13}")
    for day in [d for d in index if start <= d <= end]:
        if day not in close.index:
            continue
        c = float(close.loc[day])
        lo = float(low.loc[day]) if day in low.index else float("nan")
        halved = ma._is_halved(close, high, day)
        info = ma._score_symbol(close, high, low, day, window_start)

        best_dev = float("inf")
        best_fixed = ""
        for fixed_date in ma.FIXED_DATES:
            wstart = window_start[fixed_date]
            valid = close.loc[wstart:day].dropna()
            if valid.size < ma.MIN_WINDOW_POINTS:
                continue
            mavg = float(valid.mean())
            if mavg <= 0:
                continue
            # 收盘/最低价取更接近的一侧（lo 为 NaN 时 min 自动取收盘侧）。
            dev = min(abs(c - mavg) / mavg, abs(lo - mavg) / mavg)
            if dev < best_dev:
                best_dev, best_fixed = dev, fixed_date

        flag = "★入选" if info else ""
        if only_hits and not info:
            continue
        hits = ",".join(info["near_dates"]) if info else "-"
        score = info["score"] if info else 0
        print(
            f"{day:<10}{c:>7.2f}{'是' if halved else '否':>6}{hits:>18}"
            f"{score:>6}{best_dev:>9.2%} ({best_fixed}) {flag}"
        )


if __name__ == "__main__":
    main()
