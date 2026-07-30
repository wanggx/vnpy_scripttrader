"""基于 5 个固定日期"成本均线"的选股脚本。

对每个标的，分别计算从固定日期到最新交易日 T 的前复权收盘价均值作为一条均线，
判断当天收盘价是否在 ``NEAR_THRESHOLD`` 内接近这些均线，按"接近越多 + 时间越久
(天数越大)权重越大"打分 0-100，选前 ``TOP_N`` 存入 SqlApp 数据库。结果保留"接近
的均线有哪几根 + 每根天数"，按日期保存、仅保留最近 ``RETENTION_DAYS`` 天。

开 ``REQUIRE_HALVED`` 时，额外要求 T 日收盘价相对近 ``HIGH_LOOKBACK`` 个交易日
最高价已腰斩（``close <= 最高价 × HALF_RATIO``），未腰斩的标的在打分前整批剔除。

需 MiniQMT 运行后经 ScriptTrader 执行（与 ``download_xtquant_daily.py`` 相同约束）。
全量历史由 ``download_xtquant_for_ma.py`` 一次性下载到 xtquant 本地缓存；本脚本每轮
仅增量下载当日新日线，再用全区间读取计算均线（前复权下历史价会随分红整体位移）。

默认每个交易日收盘后 16:00 执行一次并长期循环：启动后等待下一个 16:00 才首次
执行，周末/节假日（非交易日）跳过，单轮失败等下一轮，用户停止则退出调度。
"""

from __future__ import annotations

import math
import time
import traceback
from collections import Counter
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import pandas as pd
from xtquant import xtdata

from vnpy_sqlapp import APP_NAME

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine
    from vnpy_sqlapp import SqlEngine


# ---- 可配置常量 ----
# 5 个固定日期（YYYYMMDD），从该日期到今天的前复权收盘价均值即一条均线。
FIXED_DATES: list[str] = ["20220427", "20221031", "20240205", "20240918", "20250407"]
# 标的可用前复权日线数据少于此值则不考虑（剔除上市太近、数据不足的新股）。
MIN_BARS: int = 100
# 接近标准：|close - ma| / ma <= NEAR_THRESHOLD。
NEAR_THRESHOLD: float = 0.01
# 选前 N 个标的入库。
TOP_N: int = 100
# 仅保留最近 N 天的数据。
RETENTION_DAYS: int = 7
# 单条均线窗口内有效交易日点少于此值则该均线无效（不进分子也不进分母）。
MIN_WINDOW_POINTS: int = 5
# 复权方式：front=前复权 / back=后复权 / none=不复权。
DIVIDEND_TYPE: str = "front"
# 是否排除 ST/*ST（按 InstrumentName 含 "ST" 判断）。
EXCLUDE_ST: bool = True
# 腰斩过滤：仅选 T 日收盘价相对近 HIGH_LOOKBACK 个交易日最高价已腰斩的标的
# （close <= 最高价 × HALF_RATIO）。关掉则不滤。用 high 字段取真实最高价。
REQUIRE_HALVED: bool = True
HALF_RATIO: float = 0.5
HIGH_LOOKBACK: int = 252
# 每批读取的标的数量，批间响应停止操作。
BATCH_SIZE: int = 500
# 结果表名。
TABLE_NAME: str = "stock_near_ma"
# 打分权重模式：days=天数即权重 / equal=等权(仅看接近条数) / log=对数压缩。
WEIGHT_MODE: str = "days"

# 每日定时执行：A 股 15:00 收盘，16:00 当日数据已就绪。
RUN_HOUR: int = 16
RUN_MINUTE: int = 0
# 等待时每步最长睡眠秒数，分段睡眠以快速响应停止操作。
SLEEP_STEP_SECONDS: int = 60

# xtquant 板块分类，新版用"沪深京A股"，旧版回退。
PRIMARY_SECTOR: str = "沪深京A股"
FALLBACK_SECTORS: tuple[str, ...] = ("沪深A股", "京市A股")
VALID_MARKETS: tuple[str, ...] = (".SH", ".SZ", ".BJ")


def _weight(days: int) -> float:
    """按 WEIGHT_MODE 返回单条均线的权重。"""
    if WEIGHT_MODE == "equal":
        return 1.0
    if WEIGHT_MODE == "log":
        return math.log(1 + days)
    return float(days)


def _next_run_dt(now: datetime) -> datetime:
    """返回 now 之后下一个 ``RUN_HOUR:RUN_MINUTE`` 的 datetime。

    若 now 恰好是整点则算作"下一个"（启动不立即触发，符合"等到下一个16:00"）。
    """
    candidate: datetime = now.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def _get_trading_dates(start_time: str, end_time: str) -> list[str]:
    """获取沪深交易日列表（YYYYMMDD），跳过节假日扩展下载。

    ``xtdata.get_trading_calendar`` 会无条件调 ``download_holiday_data``，
    部分客户端不支持该功能会抛 ``function not realize``。这里改用
    ``get_trading_dates`` 直接取交易所交易日时间戳（毫秒），不触发节假日
    下载——其结果与交易日历在交易日范围内一致（节假日扩展只影响非交易日）。
    """
    timestamps: list[int] = xtdata.get_trading_dates(
        market="SH", start_time=start_time, end_time=end_time, count=-1
    )
    return [datetime.fromtimestamp(ts / 1000).strftime("%Y%m%d") for ts in timestamps]


def _is_trading_day(date: datetime) -> bool:
    """date（含节假日）是否为 A 股交易日，依据 xtquant 沪市交易日。"""
    date_str: str = date.strftime("%Y%m%d")
    return date_str in _get_trading_dates(date_str, date_str)


def _sleep_until(engine: "ScriptEngine", wake_at: datetime) -> bool:
    """睡眠至 wake_at，分段以响应停止。返回是否正常醒来到点（False=被停止）。"""
    now: datetime = datetime.now()
    while now < wake_at:
        if not engine.strategy_active:
            return False
        remaining: float = (wake_at - now).total_seconds()
        step: float = min(SLEEP_STEP_SECONDS, remaining)
        time.sleep(step)
        now = datetime.now()
    return engine.strategy_active


def _normalize_bars(df: pd.DataFrame) -> pd.DataFrame:
    """把 get_market_data_ex 的单标的 DataFrame 转成收盘价/最高价 DataFrame。

    保留 ``close``、``high`` 两列；index 统一为 YYYYMMDD 字符串并按日期升序排序，
    便于 ``.loc[start:T]`` 切片与 ``.iloc[-N:]`` 取近 N 个交易日。兼容 index 为
    int 毫秒时间戳 / datetime / 字符串三种格式。
    """
    bars: pd.DataFrame = df[["close", "high"]].copy()
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


def _get_all_stock_codes(engine: "ScriptEngine") -> list[str]:
    """获取当前沪深京 A 股代码，兼容旧版板块分类。"""
    engine.write_log("正在更新 xtquant 板块分类数据")
    try:
        xtdata.download_sector_data()
    except Exception as exc:  # noqa: BLE001 - 板块刷新失败时仍尝试读取已有分类
        engine.write_log(f"板块分类更新失败（忽略）：{exc}")

    stock_codes: list[str] = xtdata.get_stock_list_in_sector(PRIMARY_SECTOR)
    if not stock_codes:
        engine.write_log(
            f"未找到“{PRIMARY_SECTOR}”板块，回退到：{', '.join(FALLBACK_SECTORS)}"
        )
        stock_codes = []
        for sector in FALLBACK_SECTORS:
            stock_codes.extend(xtdata.get_stock_list_in_sector(sector))

    # 多板块可能重复，过滤非沪深京市场的异常成分。
    return sorted({code for code in stock_codes if code.endswith(VALID_MARKETS)})


def _filter_universe(
    engine: "ScriptEngine",
    stock_codes: list[str],
) -> list[tuple[str, str]] | None:
    """筛选标的池：排除 ST/*ST 与合约信息缺失的标的。

    返回 ``[(code, name)]``；用户停止时返回 None。
    上市太近、日线数据不足的标的在打分阶段按 ``MIN_BARS`` 过滤（那里才有收盘价数据）。
    """
    engine.write_log(
        f"正在读取 {len(stock_codes)} 个标的的合约信息（排除ST）"
    )
    kept: list[tuple[str, str]] = []
    total: int = len(stock_codes)
    for index, code in enumerate(stock_codes, start=1):
        if not engine.strategy_active:
            engine.write_log(f"筛选已停止：已处理 {index - 1}/{total}")
            return None
        if index % 500 == 0:
            engine.write_log(f"筛选进度：{index}/{total}")

        detail: dict[str, Any] | None = xtdata.get_instrument_detail(code)
        if not detail:
            continue
        name: str = str(detail.get("InstrumentName", "")).strip()
        if EXCLUDE_ST and "ST" in name.upper():
            continue
        kept.append((code, name))
    return kept


def _download_today(
    engine: "ScriptEngine",
    universe: list[tuple[str, str]],
    trade_date: str,
) -> bool:
    """增量下载当日日线到 MiniQMT 本地缓存，只补当天不重下历史。

    ``get_market_data_ex`` 只读缓存、不自动下载，故每轮需先补当日新 bar。
    ``start_time=end_time=trade_date`` 只取当天，``incrementally=True`` 仅补
    缓存缺失部分（已缓存不重下），全量历史由 ``download_xtquant_for_ma.py``
    一次性拉、日常仅增量当天。单批失败记日志继续、不中断（读缓存兜底）。
    返回是否完整执行（用户停止返回 False）。
    """
    codes: list[str] = [code for code, _ in universe]
    total: int = len(codes)
    engine.write_log(f"开始增量下载当日日线（{trade_date}）：{total} 个标的")
    last_logged: list[int] = [0]

    def on_progress(data: dict[str, Any]) -> None:
        done: int = data.get("done", 0)
        total_n: int = data.get("total", total)
        if done - last_logged[0] >= 500 or done >= total_n:
            last_logged[0] = done
            engine.write_log(f"当日下载进度：{done}/{total_n}")

    for start in range(0, total, BATCH_SIZE):
        if not engine.strategy_active:
            engine.write_log(f"当日下载已停止：已补充约 {start}/{total} 个标的")
            return False

        batch: list[str] = codes[start : start + BATCH_SIZE]
        try:
            xtdata.download_history_data2(
                stock_list=batch,
                period="1d",
                start_time=trade_date,
                end_time=trade_date,
                callback=on_progress,
                incrementally=True,
            )
            engine.write_log(
                f"当日批次下载完成：已补充 {min(start + len(batch), total)}/{total} 个标的"
            )
        except Exception as exc:  # noqa: BLE001 - 单批失败记日志继续，读缓存兜底
            engine.write_log(f"当日批次下载异常（{batch[0]}~{batch[-1]}）：{exc}")
    engine.write_log("当日日线下载完成")
    return True


def _load_bar_series(
    engine: "ScriptEngine",
    universe: list[tuple[str, str]],
    start_time: str,
    end_time: str,
) -> dict[str, pd.DataFrame] | None:
    """分批读取全池前复权收盘价/最高价，返回 ``{code: DataFrame}``。

    用户停止时返回 None。先增量下载当日日线（``_download_today``，只补当天），
    再用 ``get_market_data_ex`` 读全区间缓存（前复权下每次新分红会整体位移
    历史价，均线需重读全段，不可只取增量）。``fill_data=False`` 让停牌留 NaN，
    ``dropna`` 后的长度才是真实交易日数。同时读 ``high`` 用于腰斩判断。
    """
    # 当日新 bar 不在缓存里时 get_market_data_ex 取不到，先增量补当天。
    if not _download_today(engine, universe, end_time):
        return None

    codes: list[str] = [code for code, _ in universe]
    result: dict[str, pd.DataFrame] = {}
    total: int = len(codes)
    logged_sample: bool = False

    for start in range(0, total, BATCH_SIZE):
        if not engine.strategy_active:
            engine.write_log(f"读取已停止：已补充约 {start}/{total} 个标的")
            return None

        batch: list[str] = codes[start : start + BATCH_SIZE]
        data: dict[str, pd.DataFrame] = xtdata.get_market_data_ex(
            field_list=["close", "high"],
            stock_list=batch,
            period="1d",
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type=DIVIDEND_TYPE,
            fill_data=False,
        )
        for code, df in data.items():
            if df is None or len(df) == 0:
                continue
            result[code] = _normalize_bars(df)

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


def _decide_trade_date(
    engine: "ScriptEngine",
    series_map: dict[str, pd.DataFrame],
    calendar: list[str],
) -> str:
    """确定本次计算的交易日 T。

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
    engine.write_log(
        f"未找到 >=50% 的交易日，使用众数 {best}（{freq[best]}/{total}）"
    )
    return best


def _recent_high(close: pd.Series, high: pd.Series, trade_date: str) -> float:
    """近 HIGH_LOOKBACK 个交易日的最高价（high 缺失回退 close 近似）。

    取 ``high`` 列截至 T 日最后 ``HIGH_LOOKBACK`` 个交易日的最大值；``high`` 缺失/全
    NaN 时回退用同窗口 ``close`` 最大值近似。窗口无有效数据返回 NaN。
    """
    if trade_date not in close.index:
        return float("nan")
    pos: int = close.index.get_loc(trade_date)
    if pos < 0:
        return float("nan")
    lo: int = max(0, pos - HIGH_LOOKBACK + 1)
    # 对齐 high 到 close 的 index（_normalize_bars 已保证同 index，防御性再 align）。
    tail_high: pd.Series = high.reindex(close.index).iloc[lo : pos + 1]
    if tail_high.notna().any():
        return float(tail_high.max())
    tail_close: pd.Series = close.iloc[lo : pos + 1]
    if tail_close.notna().any():
        return float(tail_close.max())
    return float("nan")


def _is_halved(close: pd.Series, high: pd.Series, trade_date: str) -> bool:
    """T 日收盘价相对近 HIGH_LOOKBACK 个交易日最高价是否已腰斩。

    窗口无有效数据视为无法判定 → 不过滤（保守不剔）。
    ``close <= high_max × HALF_RATIO`` 即腰斩。
    """
    high_max: float = _recent_high(close, high, trade_date)
    if pd.isna(high_max) or high_max <= 0:
        return True  # 窗口无有效数据，无法判定，保守不剔
    if trade_date not in close.index:
        return True
    close_t: float = float(close.loc[trade_date])
    return close_t <= high_max * HALF_RATIO


def _score_symbol(
    close: pd.Series,
    high: pd.Series,
    trade_date: str,
    window_start: dict[str, str],
) -> dict[str, Any] | None:
    """计算单标的的打分。

    返回 ``{near_dates, near_days, near_values, score, close, high52w}`` 或 None（跳过）：
    - T 日无收盘价 → 跳过整个标的
    - REQUIRE_HALVED 且未腰斩 → 跳过整个标的（硬过滤）
    - 某均线窗口有效交易日数 < MIN_WINDOW_POINTS → 该均线无效，不进分子也不进分母
    - 分母只含有效均线，避免无效均线系统性低估分数
    """
    if trade_date not in close.index:
        return None
    close_t: float = close.loc[trade_date]
    if pd.isna(close_t) or close_t <= 0:
        return None

    # 腰斩硬过滤：未腰斩的标的整批剔除，不进打分。
    if REQUIRE_HALVED and not _is_halved(close, high, trade_date):
        return None

    near_dates: list[str] = []
    near_days: list[int] = []
    near_values: list[float] = []
    near_weight: float = 0.0
    valid_weight_total: float = 0.0

    for fixed_date in FIXED_DATES:
        start: str = window_start[fixed_date]
        window: pd.Series = close.loc[start:trade_date]
        valid: pd.Series = window.dropna()
        n: int = valid.size
        if n < MIN_WINDOW_POINTS:
            continue
        ma: float = float(valid.mean())
        if ma <= 0:
            continue
        weight: float = _weight(n)
        valid_weight_total += weight
        if abs(close_t - ma) / ma <= NEAR_THRESHOLD:
            near_dates.append(fixed_date)
            near_days.append(n)
            near_values.append(round(ma, 2))
            near_weight += weight

    if valid_weight_total <= 0 or not near_dates:
        return None
    score: int = round(near_weight / valid_weight_total * 100)
    if score <= 0:
        return None
    # 近 HIGH_LOOKBACK 个交易日最高价（high 缺失则用 close 近似），用于结果展示/复盘。
    high52w: float = _recent_high(close, high, trade_date)
    return {
        "near_dates": near_dates,
        "near_days": near_days,
        "near_values": near_values,
        "score": score,
        "close": round(float(close_t), 2),
        "high52w": round(high52w, 2),
    }


def _ensure_column(
    sql_engine: "SqlEngine",
    driver: str,
    table: str,
    column: str,
    col_type: str,
    engine: "ScriptEngine",
) -> None:
    """确保表含指定列；旧表缺少该列时补列。

    ``CREATE TABLE IF NOT EXISTS`` 不会修改已有表结构，新增列时需对已存在的旧表
    执行 ``ALTER TABLE ADD COLUMN``，否则 INSERT 列数不匹配会报错。SQLite 用
    PRAGMA、MySQL/PostgreSQL 用 information_schema 判断列是否存在。
    """
    if driver == "sqlite":
        rows: list[dict[str, Any]] = sql_engine.query_all(f"PRAGMA table_info({table})")
        exists: bool = any(r.get("name") == column for r in rows)
    else:
        hit: list[dict[str, Any]] = sql_engine.query_all(
            f"SELECT 1 FROM information_schema.columns "
            f"WHERE table_name = %s AND column_name = %s",
            [table, column],
        )
        exists = bool(hit)
    if exists:
        return
    sql_engine.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    engine.write_log(f"旧表补列：{table}.{column} {col_type}")


def _save_results(
    sql_engine: "SqlEngine",
    driver: str,
    trade_date: str,
    results: list[dict[str, Any]],
    engine: "ScriptEngine",
) -> None:
    """建表、幂等写入当日结果、清理超过 RETENTION_DAYS 的旧数据。"""
    ph: str = "?" if driver == "sqlite" else "%s"

    ddl: str = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ("
        f"trade_date VARCHAR(8) NOT NULL, "
        f"code VARCHAR(16) NOT NULL, "
        f"name VARCHAR(64), "
        f"close REAL, "
        f"high52w REAL, "
        f"score INTEGER, "
        f"near_dates VARCHAR(64), "
        f"near_days VARCHAR(64), "
        f"near_values VARCHAR(64), "
        f"PRIMARY KEY (trade_date, code)"
        f")"
    )
    sql_engine.execute(ddl)
    # 已存在的旧表缺列时补列（CREATE TABLE IF NOT EXISTS 不改旧表结构）。
    _ensure_column(sql_engine, driver, TABLE_NAME, "high52w", "REAL", engine)
    _ensure_column(sql_engine, driver, TABLE_NAME, "near_values", "VARCHAR(64)", engine)

    delete_today: str = f"DELETE FROM {TABLE_NAME} WHERE trade_date = {ph}"
    insert: str = (
        f"INSERT INTO {TABLE_NAME} "
        f"(trade_date, code, name, close, high52w, score, near_dates, near_days, near_values) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})"
    )
    rows: list[tuple[Any, ...]] = [
        (
            trade_date,
            r["code"],
            r["name"],
            r["close"],
            r["high52w"],
            r["score"],
            ",".join(r["near_dates"]),
            ",".join(str(d) for d in r["near_days"]),
            ",".join(str(v) for v in r["near_values"]),
        )
        for r in results
    ]

    with sql_engine.transaction() as conn:
        conn.execute(delete_today, (trade_date,))
        if rows:
            conn.executemany(insert, rows)
    engine.write_log(f"已写入 {len(rows)} 行（trade_date={trade_date}）")

    cutoff: str = (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime("%Y%m%d")
    cleanup: str = f"DELETE FROM {TABLE_NAME} WHERE trade_date < {ph}"
    deleted: int = sql_engine.execute(cleanup, (cutoff,))
    engine.write_log(f"清理 {RETENTION_DAYS} 天前数据：删除 {deleted} 行（cutoff={cutoff}）")


def run(engine: "ScriptEngine") -> None:
    """执行一轮选股：取数 → 打分 → 选前 TOP_N → 落库 → 清理旧数据。"""
    # A. 取 SqlEngine 并判定数据库驱动（三库兼容关键）。
    sql_engine: "SqlEngine | None" = engine.main_engine.get_engine(APP_NAME)
    if sql_engine is None:
        raise RuntimeError(
            "选股脚本依赖 SqlApp，请先加载 SqlApp（script/run.py 中 add_app(SqlApp)）"
        )
    driver: str = getattr(sql_engine.database, "driver_name", "sqlite")
    engine.write_log(f"SqlApp 已就绪，数据库驱动：{driver}")

    # B. 标的池：全市场 A 股，排除 ST（日线数据不足的在打分阶段按 MIN_BARS 剔除）。
    stock_codes: list[str] = _get_all_stock_codes(engine)
    if not stock_codes:
        raise RuntimeError(
            "xtquant 未返回任何 A 股代码，请确认 MiniQMT 已启动且行情服务可用"
        )
    engine.write_log(f"全市场代码 {len(stock_codes)} 个")
    universe: list[tuple[str, str]] | None = _filter_universe(engine, stock_codes)
    if universe is None:
        engine.write_log("选股已停止（筛选阶段）")
        return
    if not universe:
        engine.write_log("筛选后无可用标的，结束")
        return
    engine.write_log(f"筛选后标的 {len(universe)} 个")

    # C. 交易日历与各均线窗口起点。
    end_date: str = datetime.now().strftime("%Y%m%d")
    calendar: list[str] = _get_trading_dates(FIXED_DATES[0], end_date)
    if not calendar:
        raise RuntimeError(
            f"xtquant 未返回 {FIXED_DATES[0]} 至 {end_date} 的交易日，请检查 xtquant 连接"
        )
    window_start: dict[str, str] = {
        fixed_date: min(d for d in calendar if d >= fixed_date)
        for fixed_date in FIXED_DATES
    }
    start_min: str = min(window_start.values())
    engine.write_log(
        f"交易日历 {len(calendar)} 个，均线窗口起点：{window_start}"
    )

    # D. 批量读取前复权收盘价/最高价（区间读到今天，便于 T 自动回退）。
    engine.write_log(f"开始读取前复权行情（{start_min} 至 {end_date}）")
    series_map: dict[str, pd.DataFrame] | None = _load_bar_series(
        engine, universe, start_min, end_date
    )
    if series_map is None:
        engine.write_log("选股已停止（读取数据阶段）")
        return
    if not series_map:
        raise RuntimeError(
            "未读到任何行情数据，请先运行 download_xtquant_for_ma.py "
            "下载全量日线到 xtquant 本地缓存"
        )
    engine.write_log(f"读到 {len(series_map)} 个标的的行情")

    # 确定本次计算交易日 T（自动适应盘中数据未就绪）。
    trade_date: str = _decide_trade_date(engine, series_map, calendar)
    engine.write_log(f"本次计算交易日 T = {trade_date}")

    # E/F. 逐标的计算均线 + 接近判断 + 打分。
    name_map: dict[str, str] = dict(universe)
    scored: list[dict[str, Any]] = []
    skipped_few_bars: int = 0
    skipped_not_halved: int = 0
    total: int = len(series_map)
    for index, (code, bars) in enumerate(series_map.items(), start=1):
        if index % 1000 == 0 and not engine.strategy_active:
            engine.write_log(f"选股已停止（打分阶段，已处理 {index}/{total}）")
            return
        # 数据不足的新股不考虑（上市太近、长期停牌致有效日线不足 MIN_BARS）。
        if bars["close"].dropna().size < MIN_BARS:
            skipped_few_bars += 1
            continue
        info: dict[str, Any] | None = _score_symbol(
            bars["close"], bars["high"], trade_date, window_start
        )
        if info is None:
            # 区分"未腰斩被剔除"与"无均线接近/数据问题"：仅在腰斩过滤开启时，
            # 用预检区分，避免对每个 None 都查一遍 high 浪费算力。
            if REQUIRE_HALVED and _is_halved(bars["close"], bars["high"], trade_date):
                continue  # 已腰斩但无均线接近 → 普通跳过
            skipped_not_halved += 1
            continue
        info["code"] = code
        info["name"] = name_map.get(code, "")
        scored.append(info)
    engine.write_log(
        f"打分完成：{len(scored)} 个标的接近至少一条均线，"
        f"因日线数据不足 {MIN_BARS} 条剔除 {skipped_few_bars} 个，"
        f"因未腰斩剔除 {skipped_not_halved} 个"
    )

    # G. 排序选前 TOP_N。
    scored.sort(key=lambda x: (-x["score"], x["code"]))
    top: list[dict[str, Any]] = scored[:TOP_N]
    if not top:
        engine.write_log("无标的接近任何均线，结束")
        return
    engine.write_log(
        f"取前 {len(top)} 个入库，最高分 {top[0]['score']}，最低分 {top[-1]['score']}"
    )

    # H. 落库并清理旧数据。
    _save_results(sql_engine, driver, trade_date, top, engine)
    engine.write_log("选股完成")


def run1(engine: "ScriptEngine") -> None:
    """ScriptTrader 策略入口：每个交易日 16:00 循环执行选股。

    - 启动后等待下一个 16:00 才首次执行（不立即触发）；
    - 周末/节假日（非交易日）跳过，等到下一个交易日 16:00；
    - 单轮异常被捕获并记日志，不影响后续轮次；
    - 用户停止（``engine.strategy_active`` 为 False）则退出调度。
    """
    engine.write_log(
        f"选股调度启动：每交易日 {RUN_HOUR:02d}:{RUN_MINUTE:02d} 执行，"
        f"非交易日跳过，等待首个触发点..."
    )
    while engine.strategy_active:
        wake_at: datetime = _next_run_dt(datetime.now())
        engine.write_log(f"下次执行时间：{wake_at.strftime('%Y-%m-%d %H:%M:%S')}")
        if not _sleep_until(engine, wake_at):
            break

        today: datetime = datetime.now()
        if not _is_trading_day(today):
            engine.write_log(f"{today.strftime('%Y-%m-%d')} 非交易日，跳过")
            continue

        try:
            run(engine)
        except Exception:  # noqa: BLE001 - 单轮失败不中断调度
            engine.write_log(f"本轮执行异常：\n{traceback.format_exc()}")
        # 循环回到顶部，计算下一个 16:00（自然顺延到次日）。
    engine.write_log("选股调度已停止")

