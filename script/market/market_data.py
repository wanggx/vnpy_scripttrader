"""通用行情数据层（大 QMT RPC）：统一「取哪些标的、哪段区间、怎么规范化」。

与 ``indicators.py`` 分工：

- 本模块只负责**取数与规范化**（探覆盖 → 补缺当日 → 分批读全区间）；
- 指标计算全部在 ``indicators.py``（纯计算、可离线跑）。

其他脚本调用示例::

    import market_data as md

    universe = [("000001.SZ", "平安银行"), ("600000.SH", "浦发银行")]
    series = md.load_ohlcv_series(engine, universe, "20240101", "20250630")
    if series is None:            # 用户点了停止
        return
    bars = series["000001.SZ"]    # index=YYYYMMDD，列=open/close/high/low/volume

    calendar = md.get_trading_dates("20240101", "20250630")
    trade_date = md.decide_trade_date(engine, series, calendar)

本模块是**唯一**的取数/交易日/标的池实现：交易日历、交易日判定、补缺当日、all-A 标的池、
ST 过滤、确定交易日 T、调度等待都在这里；``select_near_ma_xtquant`` 等业务脚本反过来
复用本模块（不再各自实现一套，避免口径漂移）。前复权下历史价会随分红整体位移，
所以每轮都读全区间、不做增量拼接。

位置：本文件在 ``script/market/`` 目录下（同目录还有 A 股标的池常量 ``a_share.py``；
指标计算在 ``script/talib/indicators.py``）。
这两个目录都**不放 ``__init__.py``**（``talib`` 若成为真包会遮蔽 TA-Lib，
``vnpy.trader.utility`` 顶层就 ``import talib``）；调用方把目录加入 ``sys.path`` 后直接
``import market_data``，例如 ``example_indicators.py`` / ``select_halt_downturn_xtquant.py``。
"""

from __future__ import annotations

import sys
import time
from collections import Counter
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

# 本文件在 script/market/ 下：把本目录（同目录的 A 股标的池常量 a_share）与上一级
# script/（bigqmt_xtdata / download_xtquant_daily 等）都加入搜索路径，
# 保证从任意位置导入本模块都可用。
_MODULE_DIR = Path(__file__).resolve().parent
_SCRIPT_DIR = _MODULE_DIR.parent
for _dir in (_MODULE_DIR, _SCRIPT_DIR):
    if str(_dir) not in sys.path:
        sys.path.insert(0, str(_dir))

import bigqmt_xtdata  # noqa: E402
import download_xtquant_daily as xt_daily  # noqa: E402
from a_share import FALLBACK_SECTORS, PRIMARY_SECTOR, VALID_MARKETS  # noqa: E402
from bigqmt_xtdata import instrument_name, xtdata  # noqa: E402

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


# 默认字段与复权方式（与 select_* 脚本一致：前复权）。
OHLCV_FIELDS: tuple[str, ...] = ("open", "close", "high", "low", "volume")
DIVIDEND_TYPE: str = "front"
# 单次读取批大小；BigQMT RPC 单次超时有限，默认与桥内 chunk 对齐。
BATCH_SIZE: int = bigqmt_xtdata.READ_BATCH_SIZE
# 是否排除 ST/*ST（按 InstrumentName 含 "ST" 判断）。
EXCLUDE_ST: bool = True
# 逐只查合约信息（取名称 / 排除 ST）时的每批标的数；BigQMT 无批量 detail 接口。
NAME_BATCH_SIZE: int = 50
# 补缺当日日线时的每批标的数；过大仍可能压垮大 QMT。
DOWNLOAD_TODAY_BATCH_SIZE: int = bigqmt_xtdata.DOWNLOAD_TODAY_BATCH_SIZE
# 等待时每步最长睡眠秒数，分段睡眠以快速响应停止操作。
SLEEP_STEP_SECONDS: int = 60


def normalize_bars(
    df: pd.DataFrame,
    fields: Sequence[str] = OHLCV_FIELDS,
) -> pd.DataFrame:
    """规范化单标的行情表：只保留 ``fields`` 列，index 统一 YYYYMMDD 字符串并升序。

    兼容 index 为 int 毫秒时间戳 / datetime / 字符串三种格式（xtquant 日线在
    MiniQMT 与 BigQMT 下并不一致），统一后可安全使用 ``.loc[start:T]`` 与
    ``.iloc[-N:]`` 切片。
    """
    bars: pd.DataFrame = df[list(fields)].copy()
    idx = bars.index
    if isinstance(idx, pd.DatetimeIndex):
        ts = idx
    elif pd.api.types.is_integer_dtype(idx):
        # xtquant 日线时间戳一般为毫秒。
        ts = pd.to_datetime(idx, unit="ms", errors="coerce")
    else:
        ts = pd.to_datetime(idx, errors="coerce")
    bars.index = ts.strftime("%Y%m%d")
    return bars.sort_index()


def load_ohlcv_series(
    engine: ScriptEngine,
    universe: list[tuple[str, str]],
    start_time: str,
    end_time: str,
    *,
    fields: Sequence[str] = OHLCV_FIELDS,
    dividend_type: str = DIVIDEND_TYPE,
    batch_size: int = BATCH_SIZE,
    download_missing: bool = True,
) -> dict[str, pd.DataFrame] | None:
    """分批读取全池行情，返回 ``{code: DataFrame}``；用户停止时返回 None。

    默认先按 ``count=1`` 探各标的本地末根日期、只对缺 ``end_time`` 的标的小批次
    补数，再一次 ``get_market_data_ex`` 读全区间（禁止一次丢全市场 download）。

    ``fields`` 决定读哪些列（只要收盘价可传 ``("close",)`` 省带宽），
    ``dividend_type`` 为复权方式（front/back/none）。
    ``download_missing=False`` 时跳过「探末根 + 补缺」这一步，直接读本地库：适用于
    上游脚本（如 16:00 的 ``select_near_ma_*``）刚补过数据、本脚本只是再读一遍的
    场景，可省掉一整轮全市场 RPC 探活。此时缺失数据不会被补，交易日由
    ``decide_trade_date`` 自动回退到上一交易日。
    """
    if download_missing:
        if not download_missing_data(engine, universe, end_time):
            return None
    else:
        engine.write_log(f"跳过补数，直接读本地日线（目标日 {end_time}）")

    codes: list[str] = [code for code, _ in universe]
    result: dict[str, pd.DataFrame] = {}
    total: int = len(codes)
    logged_sample: bool = False
    engine.write_log(
        f"开始读取行情（{start_time} 至 {end_time}，{total} 只，"
        f"复权={dividend_type}，字段={list(fields)}）"
    )

    for start in range(0, total, batch_size):
        if not engine.is_active():
            engine.write_log(f"读取已停止：已补充约 {start}/{total} 个标的")
            return None

        batch: list[str] = codes[start : start + batch_size]
        data: dict[str, pd.DataFrame] = xtdata.get_market_data_ex(
            field_list=list(fields),
            stock_list=batch,
            period="1d",
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type=dividend_type,
            fill_data=False,
        )
        for code, df in data.items():
            if df is None or len(df) == 0:
                continue
            result[code] = normalize_bars(df, fields)

        if not logged_sample:
            sample_code: str | None = next((c for c in batch if c in data), None)
            if sample_code is not None:
                sidx = data[sample_code].index
                engine.write_log(
                    f"样本 {sample_code} index dtype={sidx.dtype}, 前3={list(sidx[:3])}"
                )
                logged_sample = True

        engine.write_log(f"读取行情进度：{min(start + len(batch), total)}/{total}")

    return result


def get_trading_dates(start_time: str, end_time: str) -> list[str]:
    """获取沪深交易日列表（YYYYMMDD），跳过节假日扩展下载。

    ``xtdata.get_trading_calendar`` 会无条件调 ``download_holiday_data``，
    部分客户端不支持该功能会抛 ``function not realize``。这里改用
    ``get_trading_dates``。MiniQMT 多为毫秒时间戳，BigQMT 常直接给
    YYYYMMDD 字符串，统一经 ``bigqmt_xtdata.trading_dates_to_yyyymmdd`` 规范化。
    """
    raw = xtdata.get_trading_dates(
        market="SH", start_time=start_time, end_time=end_time, count=-1
    )
    return bigqmt_xtdata.trading_dates_to_yyyymmdd(raw)


def is_trading_day(date: datetime) -> bool:
    """``date``（含节假日）是否为 A 股交易日，依据 xtquant 沪市交易日。"""
    date_str: str = date.strftime("%Y%m%d")
    return date_str in get_trading_dates(date_str, date_str)


def next_run_dt(now: datetime, hour: int, minute: int) -> datetime:
    """返回 now 之后下一个 ``hour:minute``（触发点由调用方传入，各脚本互不影响）。

    若 now 恰好等于触发点则算作"下一个"（启动不立即触发）。
    """
    candidate: datetime = now.replace(
        hour=hour, minute=minute, second=0, microsecond=0
    )
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def sleep_until(engine: ScriptEngine, wake_at: datetime) -> bool:
    """睡眠至 wake_at，分段以响应停止。返回是否正常醒来到点（False=被停止）。"""
    now: datetime = datetime.now()
    while now < wake_at:
        if not engine.is_active():
            return False
        remaining: float = (wake_at - now).total_seconds()
        step: float = min(SLEEP_STEP_SECONDS, remaining)
        time.sleep(step)
        now = datetime.now()
    return engine.is_active()


def get_all_stock_codes(engine: ScriptEngine) -> list[str]:
    """获取当前沪深 A 股代码（过滤北交所），兼容旧版板块分类。"""
    stock_codes: list[str] = xtdata.get_stock_list_in_sector(PRIMARY_SECTOR) or []
    if not stock_codes:
        engine.write_log(
            f"未找到“{PRIMARY_SECTOR}”板块，回退到：{', '.join(FALLBACK_SECTORS)}"
        )
        for sector in FALLBACK_SECTORS:
            stock_codes.extend(xtdata.get_stock_list_in_sector(sector) or [])

    return sorted({code for code in stock_codes if code.endswith(VALID_MARKETS)})


def filter_universe(
    engine: ScriptEngine,
    stock_codes: list[str],
    *,
    exclude_st: bool = EXCLUDE_ST,
) -> list[tuple[str, str]] | None:
    """读合约信息，排除 ST/*ST。BigQMT 无批量 detail 接口，按 NAME_BATCH_SIZE 逐个查。

    返回 ``[(code, name)]``；用户停止时返回 None。
    """
    engine.write_log(f"正在读取 {len(stock_codes)} 个标的的合约信息（排除ST）")
    kept: list[tuple[str, str]] = []
    total: int = len(stock_codes)

    for start in range(0, total, NAME_BATCH_SIZE):
        if not engine.is_active():
            engine.write_log(f"筛选已停止：已处理 {start}/{total}")
            return None

        batch: list[str] = stock_codes[start : start + NAME_BATCH_SIZE]
        for code in batch:
            try:
                detail: dict[str, Any] | None = xtdata.get_instrument_detail(code)
            except Exception:  # noqa: BLE001 - 单标的失败跳过
                continue
            name: str = instrument_name(detail)
            if not name and not detail:
                continue
            if exclude_st and "ST" in name.upper():
                continue
            kept.append((code, name))

        engine.write_log(f"筛选进度：{min(start + len(batch), total)}/{total}")

    return kept


def download_missing_data(
    engine: ScriptEngine,
    universe: list[tuple[str, str]],
    trade_date: str,
) -> bool:
    """先 ``count=1`` 探本地末根，再仅对缺 ``trade_date`` 的标的分批补数。

    缺的才 ``download_history_data2``，且按小批次，禁止一次塞入全市场。
    下载失败不阻断。用户停止返回 False。
    """
    if not engine.is_active():
        engine.write_log("当日数据准备已停止（尚未开始）")
        return False

    codes: list[str] = [code for code, _ in universe]
    total: int = len(codes)
    engine.write_log(f"正在探查终端本地日线覆盖（目标日 {trade_date}，{total} 只）")
    last_dates: dict[str, str] | None = xt_daily.get_last_bar_dates(
        engine, codes, trade_date
    )
    if last_dates is None:
        return False

    need: list[str] = [
        code for code in codes if last_dates.get(code, "") < trade_date
    ]
    ready: int = total - len(need)
    engine.write_log(
        f"本地日线覆盖 {trade_date}：已有 {ready}/{total}，缺 {len(need)}"
    )
    if not need:
        return True

    batch_size: int = max(1, DOWNLOAD_TODAY_BATCH_SIZE)
    engine.write_log(
        f"开始分批补缺当日日线（{trade_date}）：{len(need)} 只，每批 {batch_size}"
    )
    for start in range(0, len(need), batch_size):
        if not engine.is_active():
            engine.write_log(f"当日补数已停止：{start}/{len(need)}")
            return False
        batch: list[str] = need[start : start + batch_size]
        try:
            xtdata.download_history_data2(
                stock_list=batch,
                period="1d",
                start_time=trade_date,
                end_time=trade_date,
                incrementally=True,
            )
        except Exception as exc:  # noqa: BLE001
            engine.write_log(
                f"当日补数批次失败 {start + 1}-{start + len(batch)}/{len(need)}：{exc}；"
                "改用当前本地已有数据继续"
            )
            return True
        engine.write_log(
            f"当日补数进度：{min(start + len(batch), len(need))}/{len(need)}"
        )

    engine.write_log(f"当日缺数补数请求已发完：{len(need)} 只")
    return True


def decide_trade_date(
    engine: ScriptEngine,
    series_map: dict[str, pd.DataFrame],
    calendar: list[str],
) -> str:
    """确定本轮计算的交易日 T。

    优先取日历中最近、且有 >=50% 标的具备收盘价的交易日（自动适应盘中数据未就绪）；
    若日历内均不满足，回退到各标的最后有效日期的众数。
    """
    last_dates: list[str] = []
    for bars in series_map.values():
        valid: pd.Series = bars["close"].dropna()
        if valid.size:
            last_dates.append(valid.index[-1])
    if not last_dates:
        raise RuntimeError("未读到任何有效收盘价数据，请检查 xtquant 本地缓存")

    freq: Counter[str] = Counter(last_dates)
    total: int = len(last_dates)
    for candidate in reversed(calendar):
        if freq.get(candidate, 0) >= total * 0.5:
            if candidate != calendar[-1]:
                engine.write_log(
                    f"当日 {calendar[-1]} 数据未就绪（仅 {freq.get(calendar[-1], 0)}/{total} 有数据），"
                    f"改用前一交易日 {candidate}"
                )
            return candidate

    best: str = max(freq, key=lambda d: (freq[d], d))
    engine.write_log(f"未找到 >=50% 的交易日，使用众数 {best}（{freq[best]}/{total}）")
    return best
