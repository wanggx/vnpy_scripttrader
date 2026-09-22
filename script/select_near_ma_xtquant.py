"""基于 5 个固定日期"成本均线"的选股脚本。

对每个标的，分别计算从固定日期到最新交易日 T 的前复权收盘价均值作为一条均线，
判断当天收盘价是否在 ``NEAR_THRESHOLD`` 内接近这些均线，按"接近越多 + 时间越久
(天数越大)权重越大"打分 0-100，选前 ``TOP_N`` 存入 SqlApp 数据库。结果保留"接近
的均线有哪几根 + 每根天数"，按日期保存，历史数据全部保留、不做清理。

开 ``REQUIRE_HALVED`` 时，额外要求 T 日收盘价相对近 ``HIGH_LOOKBACK`` 个交易日
最高价已腰斩（``close <= 最高价 × HALF_RATIO``），未腰斩的标的在打分前整批剔除。

需大 QMT + xtquant-big-convert RPC 桥运行后经 ScriptTrader 执行（不再依赖 MiniQMT）。
行情经 ``bigqmt_xtdata`` 读终端本地库：先 ``count=1`` 探各标的末根日期，缺目标日的
按小批次补数，再一次分批读全区间（禁止一次丢全市场 download）。前复权下历史价会随
分红整体位移，故每轮仍读全区间计算均线。

默认每个交易日收盘后 16:00 执行一次并长期循环：启动后等待下一个 16:00 才首次
执行，周末/节假日（非交易日）跳过，单轮失败等下一轮，用户停止则退出调度。
"""

from __future__ import annotations

import math
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

import bigqmt_xtdata
from bigqmt_xtdata import xtdata
from vnpy_sqlapp import APP_NAME

# 通用取数/交易日/调度工具与 A 股标的池常量在 script/market/ 下
# （该目录无 __init__.py，直接加入 sys.path 后 import）。
_MARKET_DIR = Path(__file__).resolve().parent / "market"
if str(_MARKET_DIR) not in sys.path:
    sys.path.insert(0, str(_MARKET_DIR))

import market_data  # noqa: E402
from a_share import VALID_MARKETS  # noqa: E402

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
# 近 N 个交易日（不含 T）若收盘跌穿该均线（低于均线超过 BREAK_THRESHOLD），
# 即使 T 日又回到均线附近也不算接近——跌穿后反抽不算有效贴近。
BREAK_LOOKBACK: int = 20
# 跌穿幅度：收盘低于当时均线超过该比例才算跌穿（3 个点）。
BREAK_THRESHOLD: float = 0.03
# 选前 N 个标的入库。
TOP_N: int = 100
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
# 每批读取的标的数量；BigQMT RPC 单次超时有限，默认与桥内 chunk 对齐。
BATCH_SIZE: int = bigqmt_xtdata.READ_BATCH_SIZE
# 结果表名。
TABLE_NAME: str = "stock_near_ma"
# xtquant 中的完整 sector 名称；多个板块用英文逗号分隔。
TARGET_SECTOR_NAME: str = "SW2半导体,SW3半导体设备,SW2贵金属,SW2小金属"
# 打分权重模式：days=天数即权重 / equal=等权(仅看接近条数) / log=对数压缩。
WEIGHT_MODE: str = "days"

# 每日定时执行：A 股 15:00 收盘，16:00 当日数据已就绪。
RUN_HOUR: int = 16
RUN_MINUTE: int = 0



def _weight(days: int) -> float:
    """按 WEIGHT_MODE 返回单条均线的权重。"""
    if WEIGHT_MODE == "equal":
        return 1.0
    if WEIGHT_MODE == "log":
        return math.log(1 + days)
    return float(days)


def _next_run_dt(now: datetime) -> datetime:
    """返回 now 之后下一个 ``RUN_HOUR:RUN_MINUTE`` 的 datetime。

    委托通用数据层；触发点用本模块自己的常量，与其它脚本互不影响。
    """
    return market_data.next_run_dt(now, RUN_HOUR, RUN_MINUTE)


def _get_trading_dates(start_time: str, end_time: str) -> list[str]:
    """获取沪深交易日列表（YYYYMMDD），委托通用数据层（跳过节假日扩展下载）。"""
    return market_data.get_trading_dates(start_time, end_time)


def _is_trading_day(date: datetime) -> bool:
    """date（含节假日）是否为 A 股交易日，依据 xtquant 沪市交易日。"""
    return market_data.is_trading_day(date)


def _sleep_until(engine: ScriptEngine, wake_at: datetime) -> bool:
    """睡眠至 wake_at，分段以响应停止。返回是否正常醒来到点（False=被停止）。"""
    return market_data.sleep_until(engine, wake_at)


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


def _get_sector_stock_codes(engine: ScriptEngine, sector_name: str) -> list[str]:
    """从 xtquant 精确获取配置 sector 的当前成分。"""
    try:
        stock_codes: list[str] = xtdata.get_stock_list_in_sector(sector_name) or []
    except Exception as exc:  # noqa: BLE001 - xtquant 异常只记录并结束本轮
        engine.write_log(f"读取 xtquant 板块“{sector_name}”失败：{exc}")
        return []

    if not stock_codes:
        engine.write_log(f"xtquant 板块“{sector_name}”不存在或没有当前成分")
        return []

    # 成分可能重复或混入其他市场，仅保留沪深 A 股标的（过滤北交所）。
    return sorted({code for code in stock_codes if code.endswith(VALID_MARKETS)})


def _filter_universe(
    engine: ScriptEngine,
    stock_codes: list[str],
) -> list[tuple[str, str]] | None:
    """筛选标的池：排除 ST/*ST 与合约信息缺失的标的（委托通用数据层）。

    返回 ``[(code, name)]``；用户停止时返回 None。exclude_st 用本模块的 ``EXCLUDE_ST``。
    上市太近、日线数据不足的标的在打分阶段按 ``MIN_BARS`` 过滤（那里才有收盘价数据）。
    """
    return market_data.filter_universe(engine, stock_codes, exclude_st=EXCLUDE_ST)


def _download_today(
    engine: ScriptEngine,
    universe: list[tuple[str, str]],
    trade_date: str,
) -> bool:
    """先 ``count=1`` 探本地末根，再仅对缺 ``trade_date`` 的标的分批补数。

    委托通用数据层（缺口才 ``download_history_data2``，小批次，失败不阻断）。
    用户停止返回 False。
    """
    return market_data.download_missing_data(engine, universe, trade_date)


def _load_bar_series(
    engine: ScriptEngine,
    universe: list[tuple[str, str]],
    start_time: str,
    end_time: str,
) -> dict[str, pd.DataFrame] | None:
    """分批读取全池前复权收盘价/最高价，返回 ``{code: DataFrame}``。

    先 ``count=1`` 探覆盖并只对缺目标日的标的分批补数，再一次
    ``get_market_data_ex`` 读全区间。用户停止返回 None。
    """
    if not _download_today(engine, universe, end_time):
        return None

    codes: list[str] = [code for code, _ in universe]
    result: dict[str, pd.DataFrame] = {}
    total: int = len(codes)
    logged_sample: bool = False
    engine.write_log(
        f"开始读取前复权行情（{start_time} 至 {end_time}，{total} 只）"
    )

    for start in range(0, total, BATCH_SIZE):
        if not engine.is_active():
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
    engine: ScriptEngine,
    series_map: dict[str, pd.DataFrame],
    calendar: list[str],
) -> str:
    """确定本次计算的交易日 T（委托通用数据层，数据未就绪自动回退前一交易日）。"""
    return market_data.decide_trade_date(engine, series_map, calendar)


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


def _broke_below_ma(valid: pd.Series, trade_date: str) -> bool:
    """近 ``BREAK_LOOKBACK`` 个交易日（不含 T）是否有收盘跌穿当时均线。

    当时均线 = 从该均线窗口起点到当天的前复权收盘价均值。
    跌穿：``close < ma × (1 - BREAK_THRESHOLD)``（默认低 3 个点），比接近带
    （1%）宽，避免把带内下沿或浅幅跌破也当成跌穿。
    """
    if trade_date not in valid.index:
        return False
    pos = valid.index.get_loc(trade_date)
    if isinstance(pos, slice):
        pos = int(pos.stop) - 1
    if pos <= 0:
        return False
    hist: pd.Series = valid.iloc[:pos]
    look: pd.Series = hist.iloc[-BREAK_LOOKBACK:]
    if look.empty:
        return False
    ma_hist: pd.Series = hist.expanding(min_periods=1).mean()
    return bool((look < ma_hist.reindex(look.index) * (1.0 - BREAK_THRESHOLD)).any())


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
    - 上市日晚于均线起点 → 该均线无效（不能把「上市以来均价」当成更早的成本均线）
    - 某均线窗口有效交易日数 < MIN_WINDOW_POINTS → 该均线无效，不进分子也不进分母
    - T 日接近该均线，但近 BREAK_LOOKBACK 日曾跌穿 → 不算接近（跌穿后反抽不计入）
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
    listed: str = str(close.dropna().index[0])

    for fixed_date in FIXED_DATES:
        start: str = window_start[fixed_date]
        # 起点当天还没上市：这条成本均线不成立，不能退化成「上市日均价」。
        if listed > start:
            continue
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
            if _broke_below_ma(valid, trade_date):
                continue
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


def _save_results(
    sql_engine: SqlEngine,
    driver: str,
    trade_date: str,
    sector_name: str,
    results: list[dict[str, Any]],
    engine: ScriptEngine,
) -> None:
    """建表、幂等写入当日结果（只覆盖同交易日同板块的旧行，不清理历史）。"""
    ph: str = "?" if driver == "sqlite" else "%s"

    ddl: str = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ("
        f"trade_date VARCHAR(8) NOT NULL, "
        f"sector_name VARCHAR(191) NOT NULL, "
        f"code VARCHAR(16) NOT NULL, "
        f"name VARCHAR(64), "
        f"close REAL, "
        f"high52w REAL, "
        f"score INTEGER, "
        f"near_dates VARCHAR(64), "
        f"near_days VARCHAR(64), "
        f"near_values VARCHAR(64), "
        f"PRIMARY KEY (trade_date, sector_name, code)"
        f")"
    )
    sql_engine.execute(ddl)

    delete_today: str = (
        f"DELETE FROM {TABLE_NAME} WHERE trade_date = {ph} AND sector_name = {ph}"
    )
    insert: str = (
        f"INSERT INTO {TABLE_NAME} "
        f"(trade_date, sector_name, code, name, close, high52w, score, "
        f"near_dates, near_days, near_values) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})"
    )
    rows: list[tuple[Any, ...]] = [
        (
            trade_date,
            r["sector_name"],
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
        conn.execute(delete_today, (trade_date, sector_name))
        if rows:
            conn.executemany(insert, rows)
    engine.write_log(
        f"板块“{sector_name}”已写入 {len(rows)} 行（trade_date={trade_date}）"
    )


def _run_sector(
    engine: ScriptEngine,
    sql_engine: SqlEngine,
    driver: str,
    sector_name: str,
) -> None:
    """对单个板块执行原有选股流程。"""
    # B. 标的池：xtquant 中的申万行业当前成分，排除 ST。
    stock_codes: list[str] = _get_sector_stock_codes(engine, sector_name)
    if not stock_codes:
        engine.write_log(f"板块“{sector_name}”没有可计算成分，跳过")
        return
    engine.write_log(f"目标板块“{sector_name}”：{len(stock_codes)} 个标的")
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
    engine.write_log(f"交易日历 {len(calendar)} 个，均线窗口起点：{window_start}")

    # D. 批量读取前复权收盘价/最高价（区间读到今天，便于 T 自动回退）。
    series_map: dict[str, pd.DataFrame] | None = _load_bar_series(
        engine, universe, start_min, end_date
    )
    if series_map is None:
        engine.write_log("选股已停止（读取数据阶段）")
        return
    if not series_map:
        raise RuntimeError(
            "未读到任何行情数据。请在大 QMT「数据管理」补全日线后重试，"
            "或运行 download_xtquant_for_ma.py（经 BigQMT RPC 尽力下载）"
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
        if index % 1000 == 0 and not engine.is_active():
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
        info["sector_name"] = sector_name
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
    _save_results(sql_engine, driver, trade_date, sector_name, top, engine)
    engine.write_log(f"板块“{sector_name}”选股完成")


def _run_once(
    engine: ScriptEngine,
    target_sector_name: str = TARGET_SECTOR_NAME,
) -> None:
    """按配置顺序逐个板块执行一次选股。

    参数：
      target_sector_name  逗号分隔的板块名称串，未传时取模块常量 TARGET_SECTOR_NAME
    """
    sql_engine: SqlEngine | None = engine.main_engine.get_engine(APP_NAME)
    if sql_engine is None:
        raise RuntimeError(
            "选股脚本依赖 SqlApp，请先加载 SqlApp（script/run.py 中 add_app(SqlApp)）"
        )
    driver: str = getattr(sql_engine.database, "driver_name", "sqlite")
    engine.write_log(f"SqlApp 已就绪，数据库驱动：{driver}")

    if not bigqmt_xtdata.ping(engine):
        engine.write_log("大 QMT RPC 不可用，本轮结束（请确认终端已加载 BIGQMT 服务端）")
        return

    sector_names: list[str] = [
        name.strip() for name in target_sector_name.split(",") if name.strip()
    ]
    if not sector_names:
        engine.write_log("target_sector_name 为空，本轮结束")
        return

    try:
        engine.write_log("正在更新大 QMT 板块分类数据")
        xtdata.download_sector_data()
    except Exception as exc:  # noqa: BLE001 - 失败不阻断，成分可能已可用
        engine.write_log(f"更新板块分类数据失败（继续用已有成分）：{exc}")

    total: int = len(sector_names)
    for index, sector_name in enumerate(sector_names, start=1):
        if not engine.is_active():
            engine.write_log("选股已停止")
            return
        engine.write_log(f"开始处理板块 {index}/{total}：{sector_name}")
        try:
            _run_sector(engine, sql_engine, driver, sector_name)
        except Exception as exc:  # noqa: BLE001 - 单板块失败继续处理其他板块
            engine.write_log(f"板块“{sector_name}”处理异常：{exc}")


def run(
    engine: ScriptEngine,
    target_sector_name: str = TARGET_SECTOR_NAME,
) -> None:
    """ScriptTrader 策略入口：每个交易日 16:00 循环执行选股。

    - 启动后等待下一个 16:00 才首次执行（不立即触发）；
    - 周末/节假日（非交易日）跳过，等到下一个交易日 16:00；
    - 单轮异常被捕获并记日志，不影响后续轮次；
    - 用户停止（``engine.is_active()`` 为 False）则退出调度。

    参数：
      target_sector_name  逗号分隔的板块名称串，未传时取模块常量 TARGET_SECTOR_NAME
    """
    engine.write_log(
        f"选股调度启动：每交易日 {RUN_HOUR:02d}:{RUN_MINUTE:02d} 执行，"
        f"非交易日跳过，等待首个触发点..."
    )
    while engine.is_active():
        wake_at: datetime = _next_run_dt(datetime.now())
        engine.write_log(f"下次执行时间：{wake_at.strftime('%Y-%m-%d %H:%M:%S')}")
        if not _sleep_until(engine, wake_at):
            break

        today: datetime = datetime.now()
        if not _is_trading_day(today):
            engine.write_log(f"{today.strftime('%Y-%m-%d')} 非交易日，跳过")
            continue

        try:
            _run_once(engine, target_sector_name)
        except Exception:  # noqa: BLE001 - 单轮失败不中断调度
            engine.write_log(f"本轮执行异常：\n{traceback.format_exc()}")
        # 循环回到顶部，计算下一个 16:00（自然顺延到次日）。
    engine.write_log("选股调度已停止")
