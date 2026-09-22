"""通用技术指标计算模块（纯计算层：只依赖 numpy / pandas，不读库、不落库、不依赖 vnpy）。

指标口径对齐国内行情软件（通达信 / 同花顺），与 TA-Lib 的差异见文末对照表。
在任意 ScriptTrader 脚本里复用::

    import indicators as ind

    # 1. 手里只有收盘价序列时（MA / EMA / MACD / RSI / BOLL 可用）
    macd_df = ind.macd(bars["close"])

    # 2. 有完整 OHLCV 表时（KDJ / ATR 需要 high / low）
    kdj_df = ind.compute("kdj", bars)

    # 3. 一次算多条，返回拼接后的 DataFrame
    frame = ind.compute_many(bars, ["macd", "kdj", "rsi", "boll"])

    # 4. 只取最新一根 K 线的指标快照，直接写选股条件
    snap = ind.latest(bars, {"macd": {}, "rsi": {"n": 6}})
    if snap["DIF"] > snap["DEA"] and snap["RSI6"] < 30:
        ...

新增指标：写一个 ``f(bars, **params) -> pd.Series | pd.DataFrame`` 的函数，在
``INDICATORS`` 里登记一行即可，``compute`` / ``compute_many`` / ``latest`` 自动可用。

位置：本文件在 ``script/talib/`` 目录下（取数层在同级的 ``script/market/market_data.py``）。
该目录**不要放 ``__init__.py``**，否则会变成名为 ``talib`` 的包、遮蔽真正的 TA-Lib
（``vnpy.trader.utility`` 顶层就 ``import talib``）。调用方先把该目录加入 ``sys.path``
再 ``import indicators``，例如 ``example_indicators.py``。
本模块不依赖 vnpy / RPC，可直接把文件拷到别处离线使用。

约定：
- ``bars`` 可以是 ``DataFrame``（列名大小写不敏感，index 为日期）或
  ``Mapping[str, Series]``；对只需要收盘价的指标，也可直接传收盘价 ``Series``；
- 返回的 Series / DataFrame 与输入同 index，窗口不足的位置为 NaN；
- 不修改入参（全部返回新对象），可安全地被多个脚本共享调用；
- **指标周期等通用参数统一在文件顶部「默认参数」区定义**（``MA_N`` / ``EMA_N`` /
  ``MACD_FAST``…``MACD_HIST_SCALE`` / ``KDJ_N``…``KDJ_FLAT_RSV`` / ``RSI_N`` /
  ``RSI_PERIODS`` / ``BOLL_N``… / ``ATR_N``）：函数签名、注册表 ``INDICATORS`` 的
  ``params`` 都用它们做默认值。业务脚本要改口径就传参覆盖，或者改这里的常量，
  不要在各脚本里另定义一份同名周期常量。

与本机自带 TA-Lib 的对应关系与差异（本模块纯 pandas 实现，口径按国内软件）：

| 本模块          | TA-Lib 对应          | 差异（已实测比对） |
| --------------- | -------------------- | ---- |
| ``macd``        | ``talib.MACD``       | DIF / DEA 完全相同；本模块 MACD 柱 = 2×(DIF-DEA)（国内习惯），talib 的 macdhist 不乘 2 |
| ``kdj``         | ``talib.STOCH``      | **无法精确复现**：talib 无 J 值，且 matype 里没有 alpha=1/N 的平滑（EMA 系数是 2/(N+1)、SMA 是等权），与通达信 ``SMA(X,N,1)`` 不同；本模块按通达信口径递推 |
| ``rsi``         | ``talib.RSI``        | 都用 Wilder 平滑（alpha=1/N），尾值完全相同；仅最前约 100 根有可见差异且指数衰减（300 根时差 1e-8） |
| ``atr``         | ``talib.ATR``        | TR 定义与 Wilder 平滑一致，尾值完全相同 |
| ``boll``        | ``talib.BBANDS``     | matype=SMA、``ddof=0`` 时完全相同；本模块默认 ``ddof=1``（国内软件 STD 口径） |
| ``ma`` / ``ema``| ``talib.SMA`` / ``talib.EMA`` | SMA 相同；EMA：talib 用前 n 根 SMA 播种，本模块首值取首个有效值（= 通达信 EMA 口径），尾值相同 |

确实想用 TA-Lib（VeighNa 自带环境已安装）也可以直接 ``import talib`` 与本模块混用，
两者只在开头若干根 K 线上有细微差别，可按上表自行交叉校验。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    # 默认参数常量（指标通用口径的唯一来源）
    "ATR_N",
    "BOLL_DDOF",
    "BOLL_K",
    "BOLL_N",
    "EMA_N",
    "KDJ_FLAT_RSV",
    "KDJ_INIT",
    "KDJ_M1",
    "KDJ_M2",
    "KDJ_N",
    "MA_N",
    "MACD_FAST",
    "MACD_HIST_SCALE",
    "MACD_SIGNAL",
    "MACD_SLOW",
    "RSI_N",
    "RSI_PERIODS",
    # 注册表与统一入口
    "INDICATORS",
    "IndicatorSpec",
    "atr",
    "available",
    "boll",
    "compute",
    "compute_many",
    "cross_over",
    "cross_under",
    "ema",
    "kdj",
    "latest",
    "ma",
    "macd",
    "rsi",
    "rsi_multi",
]

# 入参：完整行情表 / 字段映射 / （仅收盘价类指标）单列序列。
BarInput = pd.DataFrame | Mapping[str, pd.Series]
# 指标函数签名：首次入参是行情，其余为可覆盖参数。
IndicatorFunc = Callable[..., pd.Series | pd.DataFrame]


# ---- 默认参数（指标通用口径的唯一来源）----
# 业务脚本要给指标传参，优先引用这里的常量，或者干脆不传（用默认值）；
# 不要再在业务脚本里另定义一份周期常量，避免同名参数在不同脚本里口径不一致。
# MA / EMA 周期。
MA_N: int = 5
EMA_N: int = 12
# MACD：DIF = EMA(fast) - EMA(slow)，DEA = EMA(DIF, signal)，
# 柱 = hist_scale × (DIF - DEA)（hist_scale=2 为国内习惯，1 对齐 talib.MACD）。
MACD_FAST: int = 12
MACD_SLOW: int = 26
MACD_SIGNAL: int = 9
MACD_HIST_SCALE: float = 2.0
# KDJ：RSV 窗口 n，K/D 平滑周期 m1/m2，K/D 初值 init，一字/横盘时 RSV 取 flat_rsv。
KDJ_N: int = 9
KDJ_M1: int = 3
KDJ_M2: int = 3
KDJ_INIT: float = 50.0
KDJ_FLAT_RSV: float = 50.0
# RSI：单周期周期数，以及 rsi_multi 的常用多周期组合。
RSI_N: int = 14
RSI_PERIODS: tuple[int, ...] = (6, 12, 24)
# BOLL：周期 n、带宽倍数 k、标准差自由度 ddof（1=样本标准差，国内 STD 口径）。
BOLL_N: int = 20
BOLL_K: float = 2.0
BOLL_DDOF: int = 1
# ATR 周期。
ATR_N: int = 14


# ---- 基础工具 ----


def _as_frame(bars: BarInput) -> pd.DataFrame:
    """把 ``Mapping[str, Series]`` 统一成 DataFrame；DataFrame 原样返回。"""
    if isinstance(bars, pd.DataFrame):
        return bars
    return pd.DataFrame({str(key): value for key, value in bars.items()})


def _series(bars: BarInput, name: str) -> pd.Series:
    """取 ``bars`` 中名为 ``name`` 的列，返回 float64 Series（列名大小写不敏感）。

    ``bars`` 为单列 Series 时只允许取 ``close``，避免收盘价被误当成 high/low 使用。
    """
    if isinstance(bars, pd.Series):
        if name.lower() != "close":
            raise ValueError(
                f"只传入了单列序列，无法取字段 {name!r}；请改传含 OHLC 的 DataFrame"
            )
        return bars.astype("float64")

    frame = _as_frame(bars)
    lookup: dict[str, Any] = {str(col).lower(): col for col in frame.columns}
    key = name.lower()
    if key not in lookup:
        raise KeyError(f"缺少字段 {name!r}，现有列：{list(frame.columns)}")
    column = frame[lookup[key]]
    if isinstance(column, pd.DataFrame):
        raise ValueError(f"字段 {name!r} 存在重名列，请先去重")
    return column.astype("float64")


def _check_fields(bars: BarInput, fields: Sequence[str], name: str) -> None:
    """提前校验字段，缺列时立刻报出「该指标需要哪些字段 / 现在有哪些列」。"""
    if isinstance(bars, pd.Series):
        if tuple(fields) != ("close",):
            raise ValueError(
                f"指标“{name}”需要字段 {tuple(fields)}，不能只传收盘价序列"
            )
        return
    columns = _as_frame(bars).columns
    lookup = {str(col).lower() for col in columns}
    missing = [f for f in fields if f not in lookup]
    if missing:
        raise KeyError(
            f"指标“{name}”需要字段 {tuple(fields)}，缺少 {missing}；"
            f"现有列：{list(columns)}"
        )


def _smooth(series: pd.Series, n: int, init: float | None = None) -> pd.Series:
    """通达信 ``SMA(X, N, 1)`` 递推平滑：``Y = ((N-1) * Y' + X) / N``。

    ``init is None``：首值取首个有效值（``Y0 = X0``，通达信 SMA 口径）；
    否则以 ``init`` 作为前值递推首根（KDJ 的 K / D 初值 50 走这条）。
    中途 NaN 只产出 NaN、不更新状态，后续有效值从上一个状态继续（对齐停牌行为）。
    """
    values = series.to_numpy(dtype="float64")
    out = np.full(values.shape, np.nan, dtype="float64")
    keep = (n - 1) / n
    alpha = 1.0 / n
    prev: float | None = init
    for i in range(values.size):
        value = float(values[i])
        if np.isnan(value):
            continue
        prev = value if prev is None else prev * keep + value * alpha
        out[i] = prev
    return pd.Series(out, index=series.index, name=series.name, dtype="float64")


# ---- 指标 ----


def ma(bars: BarInput, n: int = MA_N, field: str = "close") -> pd.Series:
    """简单移动平均（国内 MA / SMA）；窗口不足 n 根的位置为 NaN。"""
    series = _series(bars, field)
    return series.rolling(n, min_periods=n).mean().rename(f"MA{n}")


def ema(bars: BarInput, n: int = EMA_N, field: str = "close") -> pd.Series:
    """指数移动平均，口径对齐通达信 ``EMA(X, N)``（首值取首个有效值）。"""
    series = _series(bars, field)
    return series.ewm(span=n, adjust=False).mean().rename(f"EMA{n}")


def macd(
    bars: BarInput,
    fast: int = MACD_FAST,
    slow: int = MACD_SLOW,
    signal: int = MACD_SIGNAL,
    hist_scale: float = MACD_HIST_SCALE,
) -> pd.DataFrame:
    """MACD（国内口径），返回 ``DIF`` / ``DEA`` / ``MACD`` 三列。

    - ``DIF = EMA(close, fast) - EMA(close, slow)``
    - ``DEA = EMA(DIF, signal)``
    - ``MACD``（柱）``= hist_scale * (DIF - DEA)``，国内软件默认 2 倍（``MACD_HIST_SCALE``）；
      要跟 ``talib.MACD`` 的 macdhist 对齐时传 ``hist_scale=1.0``。
    """
    close = _series(bars, "close")
    dif = (close.ewm(span=fast, adjust=False).mean()
           - close.ewm(span=slow, adjust=False).mean()).rename("DIF")
    dea = dif.ewm(span=signal, adjust=False).mean().rename("DEA")
    hist = ((dif - dea) * hist_scale).rename("MACD")
    return pd.DataFrame({"DIF": dif, "DEA": dea, "MACD": hist})


def kdj(
    bars: BarInput,
    n: int = KDJ_N,
    m1: int = KDJ_M1,
    m2: int = KDJ_M2,
    init: float = KDJ_INIT,
    flat_rsv: float = KDJ_FLAT_RSV,
) -> pd.DataFrame:
    """KDJ（通达信口径），返回 ``K`` / ``D`` / ``J`` 三列。

    - ``RSV = (C - LLV(L, n)) / (HHV(H, n) - LLV(L, n)) * 100``；
      窗口内 ``HHV == LLV``（一字板 / 完全横盘）时 RSV 取 ``flat_rsv``；
      窗口不足 n 根时按已有个数计算（``min_periods=1``，同通达信）。
    - ``K = SMA(RSV, m1, 1)``、``D = SMA(K, m2, 1)``，首值取 ``init``（默认 50）。
    - ``J = 3K - 2D``。

    注意：通达信 ``SMA(X,N,1)`` 等价 EMA 且系数为 ``1/N``（Wilder 平滑），而 TA-Lib 的
    EMA 系数是 ``2/(N+1)``、SMA 是等权，所以 ``talib.STOCH`` **无法**精确复现本口径。
    初值只影响最开始若干根（递推权重按 ``1/m1`` 指数衰减），与同花顺 / 通达信显示值在
    几十根后完全一致。
    """
    high = _series(bars, "high")
    low = _series(bars, "low")
    close = _series(bars, "close")
    low_min = low.rolling(n, min_periods=1).min()
    high_max = high.rolling(n, min_periods=1).max()
    span = (high_max - low_min).abs()

    rsv = pd.Series(np.nan, index=close.index, dtype="float64")
    usable = close.notna() & high.notna() & low.notna()
    rising = usable & (span > 0)
    rsv[rising] = (close[rising] - low_min[rising]) / span[rising] * 100.0
    rsv[usable & (span <= 0)] = flat_rsv

    k = _smooth(rsv, m1, init=init).rename("K")
    d = _smooth(k, m2, init=init).rename("D")
    j = (3.0 * k - 2.0 * d).rename("J")
    return pd.DataFrame({"K": k, "D": d, "J": j})


def rsi(bars: BarInput, n: int = RSI_N) -> pd.Series:
    """RSI（通达信口径，Wilder 平滑即 ``SMA(X, N, 1)``），返回单列 ``RSIn``。

    涨幅 / 跌幅分别做 Wilder 平滑后 ``RSI = 100 * 平均涨幅 / (平均涨幅 + 平均跌幅)``；
    两者都为 0（完全横盘、无涨无跌）时取中性值 50。停牌缺口后第一根因涨跌幅缺失为 NaN。
    """
    close = _series(bars, "close")
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = _smooth(gain, n)
    avg_loss = _smooth(loss, n)

    total = avg_gain + avg_loss
    result = pd.Series(np.nan, index=close.index, dtype="float64")
    positive = total > 0
    result[positive] = 100.0 * avg_gain[positive] / total[positive]
    result[total.notna() & (total <= 0)] = 50.0
    return result.rename(f"RSI{n}")


def rsi_multi(bars: BarInput, periods: Sequence[int] = RSI_PERIODS) -> pd.DataFrame:
    """多周期 RSI，一次返回 ``RSI6`` / ``RSI12`` / ``RSI24`` 等列。"""
    return pd.DataFrame({f"RSI{p}": rsi(bars, p) for p in periods})


def boll(
    bars: BarInput,
    n: int = BOLL_N,
    k: float = BOLL_K,
    ddof: int = BOLL_DDOF,
) -> pd.DataFrame:
    """布林带，返回 ``BOLL_UPPER`` / ``BOLL_MID`` / ``BOLL_LOWER`` 三列。

    ``ddof`` 为标准差自由度：1=样本标准差（国内软件 STD 口径，默认），0=总体标准差。
    """
    close = _series(bars, "close")
    mid = close.rolling(n, min_periods=n).mean()
    std = close.rolling(n, min_periods=n).std(ddof=ddof)
    return pd.DataFrame(
        {
            "BOLL_UPPER": mid + k * std,
            "BOLL_MID": mid,
            "BOLL_LOWER": mid - k * std,
        }
    )


def atr(bars: BarInput, n: int = ATR_N) -> pd.Series:
    """ATR 平均真实波幅（Wilder 平滑，同 ``talib.ATR``），返回单列 ``ATRn``。

    ``TR = max(H - L, |H - C'|, |L - C'|)``，首根只用 ``H - L``（无前收盘）。
    """
    high = _series(bars, "high")
    low = _series(bars, "low")
    close = _series(bars, "close")
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).rename("hl"),
            (high - prev_close).abs().rename("hc"),
            (low - prev_close).abs().rename("lc"),
        ],
        axis=1,
    ).max(axis=1)
    return _smooth(true_range, n).rename(f"ATR{n}")


def cross_over(fast: pd.Series, slow: pd.Series | float) -> pd.Series:
    """``fast`` 上穿 ``slow``（金叉）：前一根 ``fast <= slow`` 且当根 ``fast > slow``。

    返回布尔 Series；比较位置含 NaN 时按 False 处理（数据不足不产生信号）。
    """
    fast_series = fast.astype("float64")
    if isinstance(slow, pd.Series):
        slow_series = slow.reindex(fast_series.index).astype("float64")
    else:
        slow_series = pd.Series(float(slow), index=fast_series.index, dtype="float64")
    return (
        (fast_series.shift(1) <= slow_series.shift(1)) & (fast_series > slow_series)
    ).rename("cross_over")


def cross_under(fast: pd.Series, slow: pd.Series | float) -> pd.Series:
    """``fast`` 下穿 ``slow``（死叉），与 ``cross_over`` 对称。"""
    fast_series = fast.astype("float64")
    if isinstance(slow, pd.Series):
        slow_series = slow.reindex(fast_series.index).astype("float64")
    else:
        slow_series = pd.Series(float(slow), index=fast_series.index, dtype="float64")
    return (
        (fast_series.shift(1) >= slow_series.shift(1)) & (fast_series < slow_series)
    ).rename("cross_under")


# ---- 注册表与统一入口 ----


@dataclass(frozen=True)
class IndicatorSpec:
    """注册表条目：计算函数 + 依赖字段 + 默认参数。

    ``params`` 只是默认值的显式声明（便于自动生成参数与文档），调用时传入的同名
    参数会覆盖它。
    """

    func: IndicatorFunc
    fields: tuple[str, ...]
    params: Mapping[str, Any] = dataclass_field(default_factory=dict)


INDICATORS: dict[str, IndicatorSpec] = {
    "ma": IndicatorSpec(ma, ("close",), {"n": MA_N}),
    "ema": IndicatorSpec(ema, ("close",), {"n": EMA_N}),
    "macd": IndicatorSpec(
        macd,
        ("close",),
        {"fast": MACD_FAST, "slow": MACD_SLOW, "signal": MACD_SIGNAL},
    ),
    "kdj": IndicatorSpec(
        kdj,
        ("high", "low", "close"),
        {"n": KDJ_N, "m1": KDJ_M1, "m2": KDJ_M2},
    ),
    "rsi": IndicatorSpec(rsi, ("close",), {"n": RSI_N}),
    "rsi_multi": IndicatorSpec(rsi_multi, ("close",), {"periods": RSI_PERIODS}),
    "boll": IndicatorSpec(
        boll,
        ("close",),
        {"n": BOLL_N, "k": BOLL_K, "ddof": BOLL_DDOF},
    ),
    "atr": IndicatorSpec(atr, ("high", "low", "close"), {"n": ATR_N}),
}


def available() -> list[str]:
    """已注册的指标名（排序后），用于打印支持的指标清单。"""
    return sorted(INDICATORS)


def compute(name: str, bars: BarInput, **params: Any) -> pd.DataFrame:
    """按注册名算单条指标，统一返回 DataFrame（Series 自动包成单列）。

    例：``compute("rsi", bars, n=6)`` / ``compute("macd", bars["close"], fast=5)``。
    """
    if name not in INDICATORS:
        raise KeyError(f"未注册的指标 {name!r}，可用：{available()}")
    spec = INDICATORS[name]
    _check_fields(bars, spec.fields, name)
    merged: dict[str, Any] = {**spec.params, **params}
    out = spec.func(bars, **merged)
    if isinstance(out, pd.Series):
        out = out.to_frame(name=out.name or name)
    return out


def compute_many(
    bars: BarInput,
    specs: Mapping[str, Mapping[str, Any]] | Sequence[str],
) -> pd.DataFrame:
    """一次算多条指标，按列横向拼成一个 DataFrame。

    ``specs`` 可写 ``["macd", "kdj", "rsi"]``，也可对单条覆盖参数：
    ``{"macd": {"fast": 5}, "rsi": {"n": 6}}``。列名沿用各指标自己的列名。
    """
    items: list[tuple[str, Mapping[str, Any]]] = (
        list(specs.items()) if isinstance(specs, Mapping) else [(n, {}) for n in specs]
    )
    frames: list[pd.DataFrame] = [
        compute(name, bars, **dict(params)) for name, params in items
    ]
    if not frames:
        if isinstance(bars, pd.DataFrame):
            return pd.DataFrame(index=bars.index)
        return pd.DataFrame()
    return pd.concat(frames, axis=1)


def latest(
    bars: BarInput,
    specs: Mapping[str, Mapping[str, Any]] | Sequence[str],
) -> dict[str, float]:
    """算完取最后一根 K 线的指标快照 ``{列名: 值}``，便于直接写选股条件。

    数据不足导致未算出的位置保留 NaN（不会丢列），便于调用方自行判断。
    """
    frame = compute_many(bars, specs)
    if frame.empty:
        return {}
    row = frame.iloc[-1]
    return {str(col): float(value) for col, value in row.items()}
