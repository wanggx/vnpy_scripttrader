"""全市场 A 股"下跌后止跌"形态扫描（沪深A股，约 5000+ 标的）。

捕捉底部止跌企稳信号：前期有明显下跌，最近连续多日"小十字星 + 极致缩量"。
判定口径（严格档，见模块常量）：

1. 前置下跌——近 ``DOWNTURN_LOOKBACK`` 个交易日（不含 T）最高**收盘价**相对 T 日
   收盘价已回落 >= ``DOWNTURN_DROP``（确认"下跌之后"的前提）。**只看收盘价**，不用
   盘中最高价：涨跌幅要「收盘 → 收盘」才自洽，日内冲高的上影线不算前高；
2/3. **连续小十字星 或 极致缩量，满足其一即算命中**（原先要求两者同时满足）：
   - 连续小十字星——最近 ``STAR_DAYS`` 日（含 T）逐日满足：实体占振幅
     ``body/range <= BODY_RATIO``、单日振幅 ``range/close <= RANGE_RATIO``、
     上下影线都 > 0，且非停牌日（``volume>0``、``high>low``）；
   - 极致缩量——近 ``STAR_DAYS`` 日有效均量 <= 之前 ``VOL_REF_DAYS`` 日有效均量 ×
     ``VOL_SHRINK_RATIO``（两个窗口都剔除停牌/一字，口径对称）；缩量窗口有效日不足
     ``MIN_WINDOW_POINTS`` / 参照均量<=0 → 该侧视为不满足。
   两侧的原始数值（``vol_min_recent`` / ``vol_min_before``、实体振幅均值等）都照常记录，
   便于复盘 T 前后是不是该区间的“地量”。

打分 0-100：缩量程度(50%) + 十字星规整度(30%) + 连续天数(20%)；**不满足的那一侧计 0 分**，
所以只靠单一条件入选的标的最高 50 分，两者都满足才能拿高分。取前
``TOP_N`` 存入 SqlApp ``stock_halt_downturn`` 表，按交易日幂等写入、历史累积
不清理。每行同时带上 T 日的指标值（``macd_dif``/``macd_dea``/``macd_bar``、
``kdj_k``/``kdj_d``/``kdj_j``、``rsi``），随形态结果一起入库，便于复盘与后置筛选。

**结果表每个字段的含义直接写在建表语句里**（``_save_results`` 的 CREATE TABLE 带 SQL
行内注释，``.schema`` 即可查看）；表结构变动（新增/改名字段）不再自动迁移，需要时
自行处理旧表（新增字段用 ALTER，改名字段用 RENAME COLUMN）。

指标周期这些通用参数不在本文件定义：统一放在通用模块 ``script/talib/indicators.py``
顶部的「默认参数」区（``MACD_FAST`` / ``KDJ_N`` / ``RSI_N`` …），本脚本直接用它的
默认口径，这里只保留「存哪几条指标」的配置（``_INDICATOR_SPECS``）。

需大 QMT + xtquant-big-convert RPC 桥运行后经 ScriptTrader 执行。行情经
``bigqmt_xtdata`` 读终端本地库：默认**不补数**，直接分批读全区间 OHLCV
（``DOWNLOAD_MISSING=False``）——16:00 的 ``select_near_ma_*`` 已把全市场日线补到
本地，本脚本 16:30 跑，直接取数即可，省掉一整轮全市场 RPC 探活开销。单独运行
本脚本（没有前置补数）时把 ``DOWNLOAD_MISSING`` 改成 True。

默认每个交易日 16:30 执行一次并长期循环（排在 ``select_near_ma_*`` 的 16:00 之后，
等它把当日数据补好）：启动后等待下一个 16:30 才首次执行，周末/节假日（非交易日）
跳过，单轮失败等下一轮，用户停止则退出调度。

本脚本**不依赖** ``select_near_ma_*`` 的代码（只与它们在时间上错开）：取数、交易日历、
标的池、ST 过滤、调度等待全部来自通用层 ``script/market/market_data.py``。
"""

from __future__ import annotations

import math
import sys
import traceback
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

import bigqmt_xtdata
from vnpy_sqlapp import APP_NAME

# 通用模块目录：script/talib（指标计算）、script/market（行情取数 + A 股标的池常量）。
# 这两个目录都不放 __init__.py——talib 目录若有 __init__.py 会变成名为 talib 的包、
# 遮蔽真正的 TA-Lib（vnpy.trader.utility 顶层就 import talib）；统一用「把目录加入
# sys.path 再直接 import」的方式。
_TALIB_DIR = Path(__file__).resolve().parent / "talib"
_MARKET_DIR = Path(__file__).resolve().parent / "market"
for _dir in (_TALIB_DIR, _MARKET_DIR):
    if str(_dir) not in sys.path:
        sys.path.insert(0, str(_dir))

import indicators as ind  # noqa: E402
import market_data  # noqa: E402
from a_share import PRIMARY_SECTOR  # noqa: E402

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine
    from vnpy_sqlapp import SqlEngine


# ---- 可配置常量（严格档）----
# 前置下跌参照窗口（交易日，不含 T）。
DOWNTURN_LOOKBACK: int = 60
# 从近 DOWNTURN_LOOKBACK 日最高**收盘价**回落 >= 该比例才算"下跌之后"（25%）。
# 收盘价口径：不掺盘中最高价，否则上影线会把"前高"抬高、虚增跌幅。
DOWNTURN_DROP: float = 0.25
# 最近连续小十字星天数（含 T）。
STAR_DAYS: int = 4
# 实体占振幅 body/range <= 该值算"十字"（25%）。
BODY_RATIO: float = 0.25
# 单日振幅 range/close <= 该值算"小 K 线"（4%）。
RANGE_RATIO: float = 0.04
# 是否要求上下影线都 > 0（排除一字板/T 字板，保留真十字星）。
REQUIRE_BOTH_SHADOWS: bool = True
# 缩量参照窗口（交易日，不含近 STAR_DAYS）。
VOL_REF_DAYS: int = 60
# 近 STAR_DAYS 日有效均量 <= 参照窗口有效均量 × 该值算"极致缩量"（40%）。
# 与"连续小十字星"是「或」关系（两者满足其一即入选），所以这一侧不满足不会直接出局。
VOL_SHRINK_RATIO: float = 0.40
# 均量窗口剔除停牌后至少要有的有效交易日数。
MIN_WINDOW_POINTS: int = 3
# 标的可用前复权日线数据少于此值则不考虑（剔除上市太近、数据不足的新股）。
MIN_BARS: int = 120
# 复权方式：front=前复权（同 select_near_ma_*，volume 不复权原值可用）。
DIVIDEND_TYPE: str = "front"
# 每批读取的标的数量；BigQMT RPC 单次超时有限，默认与桥内 chunk 对齐。
BATCH_SIZE: int = bigqmt_xtdata.READ_BATCH_SIZE
# 是否先探本地覆盖并补缺目标日数据。默认 False：16:00 的 select_near_ma_* 已把全
# 市场日线补到本地，本脚本 16:30 直接读即可，省掉一轮全市场 RPC 探活；
# 单独跑本脚本（没有前置补数）时改成 True。
DOWNLOAD_MISSING: bool = False
# 选前 N 个标的入库。
TOP_N: int = 100
# 结果表名。
TABLE_NAME: str = "stock_halt_downturn"
# 读取区间近端日历日数（≈ 覆盖 120 交易日，够算 60 日下跌 + 60 日缩量参照 + 缓冲）。
READ_LOOKBACK_DAYS: int = 400

# 入库的指标（name -> 参数覆盖）；键名见 ``indicators.available()``，要少存哪条就删掉对应键。
# 参数留空 = 用通用默认口径：指标周期等常量统一在 ``script/talib/indicators.py`` 顶部的
# 「默认参数」区（MACD_FAST/SLOW/SIGNAL、KDJ_N/M1/M2、RSI_N…），本脚本不再自定义一份，
# 避免同名参数在两处漂移。要为本脚本单独改口径就在这里覆盖，例如 {"macd": {"fast": 5}}。
_INDICATOR_SPECS: dict[str, dict[str, Any]] = {
    "macd": {},
    "kdj": {},
    "rsi": {},
}
# 本脚本实际用的 RSI 周期：跟着上面的覆盖走（没覆盖就用通用默认），列名靠它拼。
_RSI_N: int = int(_INDICATOR_SPECS.get("rsi", {}).get("n", ind.RSI_N))
# 指标输出列 ->（入库列名, 保留小数位）。
_INDICATOR_FIELDS: dict[str, tuple[str, int]] = {
    "DIF": ("macd_dif", 4),
    "DEA": ("macd_dea", 4),
    "MACD": ("macd_bar", 4),
    "K": ("kdj_k", 2),
    "D": ("kdj_d", 2),
    "J": ("kdj_j", 2),
    f"RSI{_RSI_N}": ("rsi", 2),
}

# 每日定时执行：A 股 15:00 收盘，16:00 的 select_near_ma_* 先补数据，
# 本脚本 16:30 再跑（错开半小时，确保直接读到的就是当日完整数据）。
RUN_HOUR: int = 16
RUN_MINUTE: int = 30

# ---- 过滤漏斗（日志用）----
# 形态判定的过滤步骤：(统计键, 日志展示名)。**顺序 = ``_detect_halt`` 里的返回顺序**，
# 调用方按此顺序做「剔除 N 只 → 剩余 M 只」的累积打印；新增/调整判定分支时同步这里，
# 否则漏斗数字会错位。
_REJECT_STEPS: tuple[tuple[str, str], ...] = (
    ("no_close", "T 日无有效收盘价（或缺该日行情）"),
    ("few_hist", f"近 {DOWNTURN_LOOKBACK} 日有效收盘价不足 {MIN_WINDOW_POINTS} 个"),
    ("bad_peak", "前高收盘价异常（<=0）"),
    ("no_drop", f"自前高（最高收盘价）回落不足 {DOWNTURN_DROP:.0%}"),
    ("few_stars", f"日线不足 {STAR_DAYS} 根"),
    (
        "no_signal",
        f"近 {STAR_DAYS} 日既非连续小十字星，也未缩到参照 {VOL_SHRINK_RATIO:.0%} 以内",
    ),
    ("low_score", "综合打分<=0"),
)

# ST/*ST 过滤用通用层的 ``market_data.EXCLUDE_ST``（默认 True），不在本模块重复定义
# 以免口径漂移；要改口径改通用层。


def _tradable_mask(df: pd.DataFrame) -> pd.Series:
    """DataFrame 逐日的"有效交易日"布尔掩码（排除停牌/一字/异常日）。

    有效：volume>0、high>0、high>low（high==low 视为停牌或一字板）。
    """
    high: pd.Series = df["high"]
    low: pd.Series = df["low"]
    vol: pd.Series = df["volume"]
    return (
        vol.notna()
        & high.notna()
        & low.notna()
        & (vol > 0)
        & (high > 0)
        & (high > low)
    )


def _is_doji(row: pd.Series) -> bool:
    """单日是否为"有效小十字星"（含非停牌判定）。

    条件：非停牌、range>0、body/range <= BODY_RATIO、range/close <= RANGE_RATIO、
    （可选）上下影线都 > 0。任一不满足返回 False。
    """
    o: float = float(row["open"])
    c: float = float(row["close"])
    h: float = float(row["high"])
    low: float = float(row["low"])
    v: float = float(row["volume"])
    if pd.isna(o) or pd.isna(c) or pd.isna(h) or pd.isna(low) or pd.isna(v):
        return False
    if v <= 0 or h <= 0 or h <= low:
        return False
    rng: float = h - low
    if rng <= 0:
        return False
    if abs(c - o) / rng > BODY_RATIO:
        return False
    if rng / c > RANGE_RATIO:
        return False
    if REQUIRE_BOTH_SHADOWS and (h - max(o, c) <= 0 or min(o, c) - low <= 0):
        return False
    return True


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """（保留旧名字）委托通用数据层 ``market_data.normalize_bars``。

    保留 ``open/close/high/low/volume`` 五列；index 统一为 YYYYMMDD 字符串并升序，
    与其它脚本共用同一套规范化口径（避免各自实现漂移）。
    """
    return market_data.normalize_bars(df, market_data.OHLCV_FIELDS)


def _load_ohlcv_series(
    engine: ScriptEngine,
    universe: list[tuple[str, str]],
    start_time: str,
    end_time: str,
    download_missing: bool = DOWNLOAD_MISSING,
) -> dict[str, pd.DataFrame] | None:
    """分批读取全池前复权 OHLCV，返回 ``{code: DataFrame}``。

    委托通用数据层 ``market_data.load_ohlcv_series``；默认直接读本地数据不再补数
    （见 ``DOWNLOAD_MISSING``），补数开关由调用方传入。用户停止返回 None。
    """
    return market_data.load_ohlcv_series(
        engine,
        universe,
        start_time,
        end_time,
        fields=market_data.OHLCV_FIELDS,
        dividend_type=DIVIDEND_TYPE,
        batch_size=BATCH_SIZE,
        download_missing=download_missing,
    )


def _detect_halt(
    bars: pd.DataFrame,
    trade_date: str,
    stats: Counter[str] | None = None,
) -> dict[str, Any] | None:
    """判定单标的是否命中"下跌后止跌·小十字星+缩量"形态并打分。

    返回 ``{close_price, prior_peak_price, drop_from_peak_ratio, vol_avg_recent,
    vol_min_recent, vol_avg_before, vol_min_before, vol_shrink_ratio,
    doji_streak_days, doji_body_ratio, score, macd_dif, macd_dea, macd_bar,
    kdj_k, kdj_d, kdj_j, rsi}`` 或 None（跳过）；
    指标快照只在形态全部命中后才算（不必给全市场都算 MACD/KDJ）：
    - T 日无收盘价 / 收盘<=0 → 跳过
    - 近 DOWNTURN_LOOKBACK 日有效收盘价不足 MIN_WINDOW_POINTS → 跳过
    - T 日收盘未相对前高（最高收盘价）回落 >= DOWNTURN_DROP → 跳过
    - **小十字星与极致缩量都不满足** → 跳过（两者满足其一即入选）；其中
      · 小十字星 = 最近 STAR_DAYS 日（含 T）逐日都是有效小十字星（从 T 往前实际连续天数 >= STAR_DAYS）
      · 极致缩量 = 近 STAR_DAYS 日有效均量 <= 之前 VOL_REF_DAYS 日有效均量 × VOL_SHRINK_RATIO
        （缩量窗口有效日不足 MIN_WINDOW_POINTS / 参照均量<=0 → 该侧视为不满足）
    - 综合打分<=0 → 跳过

    每步剔除都会记到 ``stats``（键见 ``_REJECT_STEPS``，按判定顺序累计），调用方据此
    打印过滤漏斗；``stats`` 为 None 时只做判定、不统计。
    """

    def reject(reason: str) -> None:
        """记一笔"在这一步被剔除"（``stats`` 为 None 时什么也不做）。"""
        if stats is not None:
            stats[reason] += 1

    if trade_date not in bars.index:
        return reject("no_close")
    pos: int = bars.index.get_loc(trade_date)
    if isinstance(pos, slice):  # 防御：index 唯一，正常不会返回 slice
        pos = int(pos.stop) - 1
    row_t: pd.Series = bars.iloc[pos]
    close_t: float = float(row_t["close"])
    if pd.isna(close_t) or close_t <= 0:
        return reject("no_close")

    # 1. 前置下跌：近 DOWNTURN_LOOKBACK 日（不含 T）的最高**收盘价**。
    #    一律走收盘价口径（不取盘中最高价）：涨跌幅要「收盘 → 收盘」比较才自洽，
    #    日内冲高留下的上影线不算"前高"。
    lo: int = max(0, pos - DOWNTURN_LOOKBACK)
    hist: pd.DataFrame = bars.iloc[lo:pos]
    hist_valid: pd.Series = hist.loc[_tradable_mask(hist), "close"]
    if hist_valid.size < MIN_WINDOW_POINTS:
        return reject("few_hist")
    peak_close: float = float(hist_valid.max())
    if peak_close <= 0:
        return reject("bad_peak")
    if close_t > peak_close * (1.0 - DOWNTURN_DROP):
        return reject("no_drop")
    drop_pct: float = 1.0 - close_t / peak_close

    # 2/3. 连续小十字星与极致缩量：**满足其一**即算命中（原先要求两者同时满足）。
    #      两侧都先各自算出来（含入库用的原始数值），最后统一按 or 判定出局。
    star_lo: int = pos - STAR_DAYS + 1
    if star_lo < 0:
        return reject("few_stars")
    tail: pd.DataFrame = bars.iloc[star_lo : pos + 1]

    # 2a. 连续小十字星：从 T 往前数实际连续天数（含 T），不必逐日提前 return。
    streak: int = 0
    i: int = pos
    while i >= 0 and _is_doji(bars.iloc[i]):
        streak += 1
        i -= 1
    doji_ok: bool = streak >= STAR_DAYS

    # 近 STAR_DAYS 日实体/振幅均值（只统计有振幅的交易日；全无振幅则记 NULL）。
    body_ratios: list[float] = []
    for _, row in tail.iterrows():
        rng: float = float(row["high"]) - float(row["low"])
        if pd.isna(rng) or rng <= 0:
            continue
        body_ratios.append(abs(float(row["close"]) - float(row["open"])) / rng)
    body_ratio_mean: float = (
        sum(body_ratios) / len(body_ratios) if body_ratios else float("nan")
    )

    # 3a. 极致缩量：近端/参照两个窗口都只统计有效交易日（口径对称，避免停牌日凑出"缩量"）。
    tail_valid: pd.Series = tail.loc[_tradable_mask(tail), "volume"]
    vol_recent: float = float(tail_valid.mean()) if tail_valid.size else float("nan")
    vol_min: float = float(tail_valid.min()) if tail_valid.size else float("nan")
    ref_lo: int = max(0, pos - STAR_DAYS - VOL_REF_DAYS + 1)
    ref_hi: int = pos - STAR_DAYS + 1  # 不含近端窗口
    ref: pd.DataFrame = bars.iloc[ref_lo:ref_hi]
    ref_valid: pd.Series = ref.loc[_tradable_mask(ref), "volume"]
    vol_ref: float = float(ref_valid.mean()) if ref_valid.size else float("nan")
    vol_min_ref: float = float(ref_valid.min()) if ref_valid.size else float("nan")
    # 任一侧有效日不足就判不了缩量 → 记 NaN，该侧视为不满足。
    shrink_ratio: float = (
        vol_recent / vol_ref
        if vol_ref > 0 and tail_valid.size >= MIN_WINDOW_POINTS
        else float("nan")
    )
    shrink_ok: bool = (
        ref_valid.size >= MIN_WINDOW_POINTS
        and not math.isnan(shrink_ratio)
        and shrink_ratio <= VOL_SHRINK_RATIO
    )

    # 4. 两者都不满足才出局；只满足一侧的照样打分（未满足的那侧计 0 分）。
    if not doji_ok and not shrink_ok:
        return reject("no_signal")

    # 5. 打分 0-100。
    vol_score: float = 0.0
    if shrink_ok:
        vol_score = (1.0 - shrink_ratio) / (1.0 - VOL_SHRINK_RATIO)
        vol_score = max(0.0, min(1.0, vol_score))
    star_score: float = 0.0
    if doji_ok and not math.isnan(body_ratio_mean):
        star_score = max(0.0, 1.0 - body_ratio_mean)
    cont_score: float = 0.0
    if doji_ok:
        cont_score = min((streak - STAR_DAYS) / 3.0, 1.0)
    score: int = round(100.0 * (0.5 * vol_score + 0.3 * star_score + 0.2 * cont_score))
    if score <= 0:
        return reject("low_score")

    result: dict[str, Any] = {
        "close_price": round(close_t, 2),
        "prior_peak_price": round(peak_close, 2),
        "drop_from_peak_ratio": round(drop_pct, 4),
        "vol_avg_recent": _round_or_none(vol_recent, 0),
        "vol_min_recent": _round_or_none(vol_min, 0),
        "vol_avg_before": _round_or_none(vol_ref, 0),
        "vol_min_before": _round_or_none(vol_min_ref, 0),
        "vol_shrink_ratio": _round_or_none(shrink_ratio, 4),
        "doji_streak_days": streak,
        "doji_body_ratio": _round_or_none(body_ratio_mean, 4),
        "score": score,
    }
    # 5. T 日指标快照（MACD / KDJ / RSI），随形态结果一起入库。
    result.update(_indicator_snapshot(bars, trade_date))
    return result


def _log_funnel(
    engine: ScriptEngine,
    total: int,
    skipped_few_bars: int,
    reject_stats: Counter[str],
    hits: int,
) -> None:
    """打印逐级过滤漏斗：每一步剔除多少只、过滤后还剩多少只。

    顺序与 ``_detect_halt`` 的判定顺序一致（见 ``_REJECT_STEPS``）；先扣掉调用方在
    判定前剔除的「有效日线不足 MIN_BARS 条」，再按 ``_REJECT_STEPS`` 顺序累积。
    """
    left: int = total - skipped_few_bars
    engine.write_log(f"判定漏斗：起始 {total} 只")
    engine.write_log(
        f"  1) 有效日线不足 {MIN_BARS} 条（新股/长期停牌）："
        f"剔除 {skipped_few_bars} 只，剩余 {left} 只"
    )
    for no, (key, label) in enumerate(_REJECT_STEPS, start=2):
        dropped: int = reject_stats.get(key, 0)
        left -= dropped
        engine.write_log(f"  {no}) {label}：剔除 {dropped} 只，剩余 {left} 只")
    engine.write_log(f"判定完成：命中止跌形态 {hits} 个（剩余 {left}）")


def _opt_float(value: Any) -> float | None:
    """转 float；``None`` / NaN / inf 一律返回 None（写库时落 NULL）。"""
    if value is None:
        return None
    try:
        number: float = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _round_or_none(value: float, digits: int) -> float | None:
    """四舍五入到 ``digits`` 位；NaN / inf → None（写库落 NULL）。

    用于「窗口无有效数据」的字段（近端全停牌、参照窗口点不足、缩量判不了等），
    避免把 NaN 写进库。
    """
    number: float | None = _opt_float(value)
    return None if number is None else round(number, digits)


def _indicator_snapshot(bars: pd.DataFrame, trade_date: str) -> dict[str, float | None]:
    """算 T 日的 MACD / KDJ / RSI 快照，键名与入库列名一致（算不出则 None）。

    只取到 T 日为止的窗口（与形态判定同口径）；数据不足的指标
    ``indicators.latest`` 会给出 NaN，这里统一转 None 以便写 NULL。
    """
    snapshot: dict[str, float] = ind.latest(bars.loc[:trade_date], _INDICATOR_SPECS)
    values: dict[str, float | None] = {}
    for output, (column, digits) in _INDICATOR_FIELDS.items():
        number: float | None = _opt_float(snapshot.get(output))
        values[column] = None if number is None else round(number, digits)
    return values


def _save_results(
    sql_engine: SqlEngine,
    driver: str,
    trade_date: str,
    results: list[dict[str, Any]],
    engine: ScriptEngine,
) -> None:
    """建表（带列说明）、幂等写入当日结果（只覆盖同交易日的旧行，不清理历史）。"""
    ph: str = "?" if driver == "sqlite" else "%s"

    # 列说明直接写在建表 SQL 里（-- 行内注释，sqlite / mysql 都合法；sqlite 会存进
    # sqlite_master.sql，``.schema`` 可见）。注意逗号必须写在注释之前，否则会被 `--` 注释掉。
    ddl: str = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n"
        f"trade_date VARCHAR(8) NOT NULL,  -- 交易日 T（YYYYMMDD）\n"
        f"code VARCHAR(16) NOT NULL,  -- 标的代码，如 000001.SZ\n"
        f"name VARCHAR(64),  -- 标的名称（已排除 ST/*ST）\n"
        f"close_price REAL,  -- T 日收盘价（前复权）\n"
        f"prior_peak_price REAL,  -- 下跌前高点：近 {DOWNTURN_LOOKBACK} 日（不含 T）最高收盘价（收盘口径）\n"
        f"drop_from_peak_ratio REAL,  -- 相对前高回落比例，>= {DOWNTURN_DROP} 才入选\n"
        f"vol_avg_recent REAL,  -- 近 {STAR_DAYS} 日有效交易日平均成交量（剔除停牌）\n"
        f"vol_min_recent REAL,  -- 近 {STAR_DAYS} 日最小成交量（地量，剔除停牌）\n"
        f"vol_avg_before REAL,  -- 之前 {VOL_REF_DAYS} 日平均成交量（剔除停牌）\n"
        f"vol_min_before REAL,  -- 之前 {VOL_REF_DAYS} 日最小成交量（剔除停牌）\n"
        f"vol_shrink_ratio REAL,  -- 缩量程度 = recent/before；<= {VOL_SHRINK_RATIO} 即算满足缩量（与十字星满足其一即可）\n"
        f"doji_streak_days INTEGER,  -- 从 T 往前实际连续小十字星天数\n"
        f"doji_body_ratio REAL,  -- 近 {STAR_DAYS} 日实体/振幅均值（只统计有振幅的日子；越小越规整）\n"
        f"macd_dif REAL,  -- MACD 快线 DIF = EMA(close,{ind.MACD_FAST}) - EMA(close,{ind.MACD_SLOW})\n"
        f"macd_dea REAL,  -- MACD 慢线 DEA = EMA(DIF,{ind.MACD_SIGNAL})\n"
        f"macd_bar REAL,  -- MACD 柱 = 2 × (DIF - DEA)\n"
        f"kdj_k REAL,  -- KDJ 的 K = SMA(RSV,{ind.KDJ_M1},1)\n"
        f"kdj_d REAL,  -- KDJ 的 D = SMA(K,{ind.KDJ_M2},1)\n"
        f"kdj_j REAL,  -- KDJ 的 J = 3K - 2D\n"
        f"rsi REAL,  -- RSI（Wilder 平滑，周期 {_RSI_N}）\n"
        f"score INTEGER,  -- 综合打分 0-100（缩量 50% + 十字星 30% + 连续天数 20%）\n"
        f"PRIMARY KEY (trade_date, code)\n"
        f")"
    )
    sql_engine.execute(ddl)

    delete_today: str = f"DELETE FROM {TABLE_NAME} WHERE trade_date = {ph}"
    insert: str = (
        f"INSERT INTO {TABLE_NAME} "
        f"(trade_date, code, name, close_price, prior_peak_price, drop_from_peak_ratio, "
        f"vol_avg_recent, vol_min_recent, vol_avg_before, vol_min_before, vol_shrink_ratio, "
        f"doji_streak_days, doji_body_ratio, macd_dif, macd_dea, macd_bar, "
        f"kdj_k, kdj_d, kdj_j, rsi, score) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, "
        f"{ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})"
    )
    rows: list[tuple[Any, ...]] = [
        (
            trade_date,
            r["code"],
            r["name"],
            r["close_price"],
            r["prior_peak_price"],
            r["drop_from_peak_ratio"],
            r["vol_avg_recent"],
            r["vol_min_recent"],
            r["vol_avg_before"],
            r["vol_min_before"],
            r["vol_shrink_ratio"],
            r["doji_streak_days"],
            r["doji_body_ratio"],
            r["macd_dif"],
            r["macd_dea"],
            r["macd_bar"],
            r["kdj_k"],
            r["kdj_d"],
            r["kdj_j"],
            r["rsi"],
            r["score"],
        )
        for r in results
    ]

    with sql_engine.transaction() as conn:
        conn.execute(delete_today, (trade_date,))
        if rows:
            conn.executemany(insert, rows)
    engine.write_log(f"已写入 {len(rows)} 行（trade_date={trade_date}）")


def _run_once(engine: ScriptEngine, download_missing: bool | None = None) -> None:
    """对沪深 A 股全市场执行一次止跌形态扫描。

    ``download_missing`` 为 None 时用模块常量 ``DOWNLOAD_MISSING``（默认不补数：
    当日数据由 16:00 的 ``select_near_ma_*`` 已补好）；手动补跑可传 True。
    """
    if download_missing is None:
        download_missing = DOWNLOAD_MISSING
    engine.write_log(
        "本轮数据准备："
        + ("先探本地覆盖并补缺当日数据" if download_missing else "直接读本地数据（不补数）")
    )

    sql_engine: SqlEngine | None = engine.main_engine.get_engine(APP_NAME)
    if sql_engine is None:
        raise RuntimeError(
            "扫描脚本依赖 SqlApp，请先加载 SqlApp（script/run.py 中 add_app(SqlApp)）"
        )
    driver: str = getattr(sql_engine.database, "driver_name", "sqlite")
    engine.write_log(f"SqlApp 已就绪，数据库驱动：{driver}")

    if not bigqmt_xtdata.ping(engine):
        engine.write_log("大 QMT RPC 不可用，本轮结束")
        return

    # 板块成分直接读桥接端已有缓存（大 QMT 桥不提供 download_sector_data）。
    stock_codes: list[str] = market_data.get_all_stock_codes(engine)
    if not stock_codes:
        engine.write_log("大 QMT 未返回任何 A 股代码，本轮结束")
        return
    engine.write_log(f"全市场标的池“{PRIMARY_SECTOR}”：{len(stock_codes)} 个")

    universe: list[tuple[str, str]] | None = market_data.filter_universe(
        engine, stock_codes
    )
    if universe is None:
        engine.write_log("扫描已停止（筛选阶段）")
        return
    if not universe:
        engine.write_log("筛选后无可用标的，结束")
        return
    engine.write_log(f"筛选后标的 {len(universe)} 个")

    end_date: str = datetime.now().strftime("%Y%m%d")
    start_date: str = (datetime.now() - timedelta(days=READ_LOOKBACK_DAYS)).strftime(
        "%Y%m%d"
    )
    calendar: list[str] = market_data.get_trading_dates(start_date, end_date)
    if not calendar:
        raise RuntimeError(
            f"大 QMT 未返回 {start_date} 至 {end_date} 的交易日，请检查 RPC"
        )
    engine.write_log(f"交易日历 {len(calendar)} 个，读取区间 {start_date} 至 {end_date}")

    series_map: dict[str, pd.DataFrame] | None = _load_ohlcv_series(
        engine, universe, start_date, end_date, download_missing=download_missing
    )
    if series_map is None:
        engine.write_log("扫描已停止（读取数据阶段）")
        return
    if not series_map:
        raise RuntimeError(
            "未读到任何行情数据。请在大 QMT「数据管理」补全日线后重试"
        )
    engine.write_log(f"读到 {len(series_map)} 个标的的行情")

    # 确定本次计算交易日 T（自动适应盘中数据未就绪）。
    trade_date: str = market_data.decide_trade_date(engine, series_map, calendar)
    engine.write_log(f"本次计算交易日 T = {trade_date}")

    name_map: dict[str, str] = dict(universe)
    hits: list[dict[str, Any]] = []
    skipped_few_bars: int = 0
    reject_stats: Counter[str] = Counter()  # 各过滤步骤剔除的标的数（漏斗日志用）
    total: int = len(series_map)
    for index, (code, bars) in enumerate(series_map.items(), start=1):
        if index % 1000 == 0:
            engine.write_log(f"扫描进度：{index}/{total}")
            if not engine.is_active():
                engine.write_log(f"扫描已停止（判定阶段，已处理 {index}/{total}）")
                return
        # 数据不足的新股/长期停牌不考虑。
        if bars["close"].dropna().size < MIN_BARS:
            skipped_few_bars += 1
            continue
        info: dict[str, Any] | None = _detect_halt(bars, trade_date, reject_stats)
        if info is None:
            continue
        info["code"] = code
        info["name"] = name_map.get(code, "")
        hits.append(info)
    _log_funnel(engine, total, skipped_few_bars, reject_stats, len(hits))

    hits.sort(key=lambda x: (-x["score"], x["code"]))
    top: list[dict[str, Any]] = hits[:TOP_N]
    if not top:
        engine.write_log("无标的命中止跌形态，结束")
        return
    engine.write_log(
        f"取前 {len(top)} 个入库，最高分 {top[0]['score']}，最低分 {top[-1]['score']}"
    )

    _save_results(sql_engine, driver, trade_date, top, engine)
    engine.write_log("全市场止跌形态扫描完成")


def _next_run_dt(now: datetime) -> datetime:
    """返回 now 之后下一个 ``RUN_HOUR:RUN_MINUTE``（本脚本自己的 16:30）。

    触发点用本模块自己的常量（不是 select_near_ma_* 的 16:00），所以两个脚本互不影响。
    """
    return market_data.next_run_dt(now, RUN_HOUR, RUN_MINUTE)


def run(engine: ScriptEngine) -> None:
    """ScriptTrader 策略入口：每个交易日 16:30 对全市场 A 股扫描止跌形态。

    16:30 排在 ``select_near_ma_*`` 的 16:00 之后，等它把当日全市场日线补到本地，
    本脚本直接读本地数据（``DOWNLOAD_MISSING=False``），不再自己补数。

    - 启动后等待下一个 16:30 才首次执行（不立即触发）；
    - 周末/节假日（非交易日）跳过，等到下一个交易日 16:30；
    - 单轮异常被捕获并记日志，不影响后续轮次；
    - 用户停止（``engine.is_active()`` 为 False）则退出调度。
    """
    engine.write_log(
        f"止跌形态扫描调度启动：每交易日 {RUN_HOUR:02d}:{RUN_MINUTE:02d} 执行"
        f"（排在 select_near_ma_* 的 16:00 补数之后），标的池={PRIMARY_SECTOR}，"
        f"数据源=大QMT RPC，直接读本地数据不补数，非交易日跳过，等待首个触发点..."
    )
    while engine.is_active():
        wake_at: datetime = _next_run_dt(datetime.now())
        engine.write_log(f"下次执行时间：{wake_at.strftime('%Y-%m-%d %H:%M:%S')}")
        if not market_data.sleep_until(engine, wake_at):
            break

        today: datetime = datetime.now()
        if not market_data.is_trading_day(today):
            engine.write_log(f"{today.strftime('%Y-%m-%d')} 非交易日，跳过")
            continue

        try:
            _run_once(engine)
        except Exception:  # noqa: BLE001 - 单轮失败不中断调度
            engine.write_log(f"本轮执行异常：\n{traceback.format_exc()}")
    engine.write_log("止跌形态扫描调度已停止")
