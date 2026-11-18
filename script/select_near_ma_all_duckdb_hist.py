"""全市场成本均线选股：从 ``vnpy.duckdb`` 日线按天回放近一年。

打分规则、均线日期、腰斩过滤、入库表与 ``select_near_ma_all_xtquant.py`` /
``select_near_ma_xtquant.py`` 完全相同（复用 ``_score_symbol`` / ``_save_results``），
区别只在：

- **取数**：读 DataManager 的 ``dbbardata``（``interval=d``），不走 BigQMT / xtquant；
- **调度**：启动后立刻按交易日逐日回放，跑完结束（不做 16:00 循环）；
- **区间**：默认回放「库内最新日线日」往前约一年的每个交易日；
- **落库**：仍写入 SqlApp 的 ``stock_near_ma``（默认与行情同库 ``vnpy.duckdb``），
  ``sector_name=沪深A股``，按 ``(trade_date, sector_name, code)`` 幂等覆盖。

用法：在 ScriptTrader 里加载本脚本后点运行；可选参数 ``lookback_days`` /
``start_date`` / ``end_date`` / ``skip_existing``（见 ``run`` 签名，界面可改）。
指定 ``start_date``/``end_date``（YYYYMMDD）时按区间回放，忽略 ``lookback_days``。
"""

# pylint: disable=protected-access
from __future__ import annotations

import sys
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

import select_near_ma_xtquant as ma
from vnpy.trader.constant import Exchange, Interval
from vnpy_sqlapp import APP_NAME

_MARKET_DIR = Path(__file__).resolve().parent / "market"
if str(_MARKET_DIR) not in sys.path:
    sys.path.insert(0, str(_MARKET_DIR))

from a_share import PRIMARY_SECTOR  # noqa: E402

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine
    from vnpy_sqlapp import SqlEngine


SECTOR_NAME: str = PRIMARY_SECTOR
EXCLUDE_CODE_PREFIXES: tuple[str, ...] = ("688", "689")

# 回放窗口：相对库内最新日线日往前多少个自然日（约一年）。
LOOKBACK_DAYS: int = 365
# 已有同交易日同板块结果时是否跳过（便于中断后续跑）。
SKIP_EXISTING: bool = True

_TRUE_TEXT: frozenset[str] = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE_TEXT: frozenset[str] = frozenset({"0", "false", "f", "no", "n", "off"})

_EXCHANGE_TO_XT: dict[str, str] = {
    Exchange.SSE.value: ".SH",
    Exchange.SZSE.value: ".SZ",
}


def _coerce_bool(value: Any, default: bool) -> bool:
    """ScriptTrader 参数常经文本框回传，显式解析 bool。"""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text: str = str(value).strip().lower()
    if text in _TRUE_TEXT:
        return True
    if text in _FALSE_TEXT:
        return False
    return default


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_xt_code(symbol: str, exchange: str) -> str | None:
    """dbbardata 的 symbol+exchange → xtquant 风格代码（000001.SZ）。"""
    suffix: str | None = _EXCHANGE_TO_XT.get(exchange)
    if suffix is None:
        return None
    if symbol.startswith(EXCLUDE_CODE_PREFIXES):
        return None
    return f"{symbol}{suffix}"


def _get_contract_names(engine: ScriptEngine) -> dict[str, str]:
    """MainEngine 合约表 → {xt_code: name}；无网关时返回空（名称落空串，不挡回放）。"""
    from vnpy.trader.constant import Exchange as Ex

    xt_suffix: dict[Ex, str] = {Ex.SSE: ".SH", Ex.SZSE: ".SZ"}
    names: dict[str, str] = {}
    for contract in engine.main_engine.get_all_contracts():
        suffix: str | None = xt_suffix.get(contract.exchange)
        if suffix is None or not contract.name:
            continue
        names[f"{contract.symbol}{suffix}"] = contract.name
    return names


def _build_universe(
    engine: ScriptEngine,
    codes: list[str],
) -> list[tuple[str, str]]:
    """构造 ``[(code, name)]``；合约表有则填名并可选剔 ST，否则 name 为空仍保留。"""
    names: dict[str, str] = _get_contract_names(engine)
    kept: list[tuple[str, str]] = []
    skipped_st: int = 0
    for code in codes:
        name: str = names.get(code, "")
        if ma.EXCLUDE_ST and name and "ST" in name.upper():
            skipped_st += 1
            continue
        kept.append((code, name))
    if skipped_st:
        engine.write_log(f"按合约名剔除 ST/*ST：{skipped_st} 个")
    if not names:
        engine.write_log(
            "MainEngine 合约表为空：名称列将写空；未做 ST 过滤"
            "（建议入口加载会推送合约的行情网关）"
        )
    return kept


def _fetch_bar_rows(
    engine: ScriptEngine,
    sql_engine: SqlEngine,
    start: datetime,
    end: datetime,
) -> list[tuple]:
    """用 SqlApp 同一连接读 ``dbbardata``，避免再开 get_database() 双连同库锁死。"""
    # DuckDB 参数用 ?；把 datetime 收成字符串，驱动更稳。
    sql: str = """
        SELECT symbol, exchange, datetime,
               CAST(high_price AS DOUBLE) AS high_price,
               CAST(low_price AS DOUBLE) AS low_price,
               CAST(close_price AS DOUBLE) AS close_price
        FROM dbbardata
        WHERE interval = ?
          AND exchange IN ('SSE', 'SZSE')
          AND datetime >= ?
          AND datetime <= ?
        ORDER BY symbol, exchange, datetime
    """
    params: tuple = (
        Interval.DAILY.value,
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S"),
    )
    engine.write_log(
        f"从 vnpy.duckdb 批量读取日线：{start.date()} ~ {end.date()}"
    )
    result = sql_engine.query(sql, params, max_rows=None)
    if result.error:
        raise RuntimeError(f"读取 dbbardata 失败：{result.error}")
    return list(result.rows)


def _load_series_from_duckdb(
    engine: ScriptEngine,
    sql_engine: SqlEngine,
    start: datetime,
    end: datetime,
) -> dict[str, pd.DataFrame]:
    """一次 SQL 拉取全市场日线 close/high/low，返回 ``{xt_code: DataFrame}``。

    index=YYYYMMDD。经 SqlApp 读库，不另开 ``get_database()`` 连接。
    """
    rows: list[tuple] = _fetch_bar_rows(engine, sql_engine, start, end)
    engine.write_log(f"原始行数 {len(rows):,}，正在按标的组装…")
    buckets: dict[str, list[tuple[str, float, float, float]]] = defaultdict(list)
    skipped_prefix: int = 0
    for symbol, exchange, dt, high, low, close in rows:
        code: str | None = _to_xt_code(str(symbol), str(exchange))
        if code is None:
            if str(symbol).startswith(EXCLUDE_CODE_PREFIXES):
                skipped_prefix += 1
            continue
        if isinstance(dt, datetime):
            day: str = dt.strftime("%Y%m%d")
        else:
            day = pd.Timestamp(dt).strftime("%Y%m%d")
        buckets[code].append(
            (day, float(high or 0), float(low or 0), float(close or 0))
        )

    series_map: dict[str, pd.DataFrame] = {}
    for code, items in buckets.items():
        frame = pd.DataFrame(items, columns=["date", "high", "low", "close"])
        frame = frame.drop_duplicates(subset=["date"], keep="last")
        frame = frame.set_index("date").sort_index()
        series_map[code] = frame

    if skipped_prefix:
        engine.write_log(
            f"已跳过科创板行（{'/'.join(EXCLUDE_CODE_PREFIXES)}）：约 {skipped_prefix} 行"
        )
    engine.write_log(f"组装完成：{len(series_map)} 个标的")
    return series_map


def _build_calendar(series_map: dict[str, pd.DataFrame]) -> list[str]:
    """用全市场日线日期并集作为交易日历（库内有 bar 的日期即交易日）。"""
    dates: set[str] = set()
    for bars in series_map.values():
        dates.update(bars.index.tolist())
    return sorted(dates)


def _existing_trade_dates(sql_engine: SqlEngine, driver: str, sector_name: str) -> set[str]:
    """已写入 ``stock_near_ma`` 的交易日集合（同板块）。"""
    ph: str = "?" if driver in ("sqlite", "duckdb") else "%s"
    result = sql_engine.query(
        f"SELECT DISTINCT trade_date FROM {ma.TABLE_NAME} "
        f"WHERE sector_name = {ph}",
        (sector_name,),
        max_rows=None,
    )
    if result.error:
        return set()
    return {str(row[0]) for row in result.rows if row and row[0]}


def _score_one_day(
    engine: ScriptEngine,
    series_map: dict[str, pd.DataFrame],
    name_map: dict[str, str],
    trade_date: str,
    window_start: dict[str, str],
) -> list[dict[str, Any]]:
    """对单个交易日 T 打分，返回排序后的前 ``TOP_N``。"""
    scored: list[dict[str, Any]] = []
    for code, bars in series_map.items():
        if trade_date not in bars.index:
            continue
        if bars.loc[:trade_date, "close"].dropna().size < ma.MIN_BARS:
            continue
        info: dict[str, Any] | None = ma._score_symbol(
            bars["close"], bars["high"], bars["low"], trade_date, window_start
        )
        if info is None:
            continue
        info["code"] = code
        info["name"] = name_map.get(code, "")
        info["sector_name"] = SECTOR_NAME
        scored.append(info)

    scored.sort(key=lambda x: (-x["score"], x["code"]))
    return scored[: ma.TOP_N]


def _normalize_ymd(value: Any) -> str:
    """把 YYYYMMDD / YYYY-MM-DD / 空 规范成 YYYYMMDD；空串表示未指定。"""
    if value is None:
        return ""
    text: str = str(value).strip()
    if not text:
        return ""
    digits: str = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 8:
        raise ValueError(f"日期须为 YYYYMMDD，收到：{value!r}")
    datetime.strptime(digits, "%Y%m%d")  # 校验合法
    return digits


def _run_hist(
    engine: ScriptEngine,
    *,
    lookback_days: int,
    skip_existing: bool,
    start_date: str = "",
    end_date: str = "",
) -> None:
    """按天回放并落库；可指定区间或 lookback。"""
    sql_engine: SqlEngine | None = engine.main_engine.get_engine(APP_NAME)
    if sql_engine is None:
        raise RuntimeError(
            "本脚本依赖 SqlApp，请先加载 SqlApp（script/run.py 中 add_app(SqlApp)）"
        )
    driver: str = getattr(sql_engine.database, "driver_name", "sqlite")
    engine.write_log(f"SqlApp 已就绪，驱动={driver}；行情源=vnpy.duckdb.dbbardata")

    start_date = _normalize_ymd(start_date)
    end_date = _normalize_ymd(end_date)

    # 均线窗口起点可能早于回放起点，取数从最早 FIXED_DATE 开始。
    data_start: datetime = datetime.strptime(min(ma.FIXED_DATES), "%Y%m%d")
    data_end: datetime = datetime.now().replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    if end_date:
        data_end = datetime.strptime(end_date, "%Y%m%d").replace(
            hour=23, minute=59, second=59
        )

    series_map: dict[str, pd.DataFrame] = _load_series_from_duckdb(
        engine, sql_engine, data_start, data_end
    )
    if not series_map:
        raise RuntimeError("vnpy.duckdb 无可用沪深日线，请先用 DataManager 写入 dbbardata")

    universe: list[tuple[str, str]] = _build_universe(
        engine, sorted(series_map.keys())
    )
    name_map: dict[str, str] = dict(universe)
    # 合约表剔 ST 后，series 也跟着缩；无名称时 universe≈全量 keys。
    if len(universe) < len(series_map):
        keep: set[str] = {code for code, _ in universe}
        series_map = {c: s for c, s in series_map.items() if c in keep}
    engine.write_log(f"标的池“{SECTOR_NAME}”：{len(series_map)} 个")

    calendar: list[str] = _build_calendar(series_map)
    if not calendar:
        raise RuntimeError("未能从日线推导交易日历")
    window_start: dict[str, str] = {
        fixed_date: min(d for d in calendar if d >= fixed_date)
        for fixed_date in ma.FIXED_DATES
        if any(d >= fixed_date for d in calendar)
    }
    if len(window_start) != len(ma.FIXED_DATES):
        missing = [d for d in ma.FIXED_DATES if d not in window_start]
        raise RuntimeError(f"交易日历覆盖不足，缺均线起点：{missing}")

    if start_date or end_date:
        lo: str = start_date or calendar[0]
        hi: str = end_date or calendar[-1]
        trade_dates: list[str] = [d for d in calendar if lo <= d <= hi]
        range_desc: str = f"区间 {lo}~{hi}"
    else:
        latest: str = calendar[-1]
        hist_floor: str = (
            datetime.strptime(latest, "%Y%m%d") - timedelta(days=lookback_days)
        ).strftime("%Y%m%d")
        trade_dates = [d for d in calendar if d >= hist_floor]
        range_desc = f"lookback={lookback_days}"
    if not trade_dates:
        raise RuntimeError(f"回放区间无交易日（{range_desc}）")
    engine.write_log(
        f"交易日历 {len(calendar)} 日；回放 {len(trade_dates)} 日"
        f"（{trade_dates[0]} ~ {trade_dates[-1]}，{range_desc}）"
    )

    done: set[str] = set()
    if skip_existing:
        done = _existing_trade_dates(sql_engine, driver, SECTOR_NAME)
        if done:
            engine.write_log(f"已存在结果 {len(done)} 个交易日，将跳过")

    total_days: int = len(trade_dates)
    written_days: int = 0
    skipped_days: int = 0
    empty_days: int = 0

    for index, trade_date in enumerate(trade_dates, start=1):
        if not engine.is_active():
            engine.write_log(
                f"回放已停止：{index - 1}/{total_days}，已写入 {written_days} 日"
            )
            return
        if trade_date in done:
            skipped_days += 1
            continue

        top: list[dict[str, Any]] = _score_one_day(
            engine, series_map, name_map, trade_date, window_start
        )
        if not top:
            empty_days += 1
        else:
            ma._save_results(sql_engine, driver, trade_date, SECTOR_NAME, top, engine)
            written_days += 1

        if index % 20 == 0 or index == total_days:
            engine.write_log(
                f"回放进度：{index}/{total_days}（写入 {written_days}，"
                f"跳过 {skipped_days}，空选 {empty_days}），当前 T={trade_date}"
            )

    engine.write_log(
        f"回放完成：写入 {written_days} 日，跳过已有 {skipped_days} 日，"
        f"空选 {empty_days} 日，表={ma.TABLE_NAME}，板块={SECTOR_NAME}"
    )


def run(
    engine: ScriptEngine,
    lookback_days: int = LOOKBACK_DAYS,
    skip_existing: bool = SKIP_EXISTING,
    start_date: str = "",
    end_date: str = "",
) -> None:
    """ScriptTrader 入口：从 duckdb 按天回放成本均线选股后退出。"""
    days: int = _coerce_int(lookback_days, LOOKBACK_DAYS)
    skip: bool = _coerce_bool(skip_existing, SKIP_EXISTING)
    start: str = _normalize_ymd(start_date)
    end: str = _normalize_ymd(end_date)
    engine.write_log(
        f"duckdb 历史回放启动：标的池={SECTOR_NAME}，lookback_days={days}，"
        f"start_date={start or '-'}，end_date={end or '-'}，"
        f"skip_existing={skip}，数据源=vnpy.duckdb，跑完即结束"
    )
    try:
        _run_hist(
            engine,
            lookback_days=days,
            skip_existing=skip,
            start_date=start,
            end_date=end,
        )
    except Exception:  # noqa: BLE001 - 异常打日志后正常结束脚本
        engine.write_log(f"回放异常：\n{traceback.format_exc()}")
    engine.write_log("duckdb 历史回放结束")


def _cli_main() -> None:
    """命令行直接跑：``python select_near_ma_all_duckdb_hist.py 20260901 20261031``。"""
    import argparse

    from vnpy.event import EventEngine
    from vnpy.trader.engine import MainEngine
    from vnpy_scripttrader import ScriptTraderApp
    from vnpy_sqlapp import SqlApp

    parser = argparse.ArgumentParser(description="duckdb 成本均线历史回放")
    parser.add_argument("start_date", nargs="?", default="20260901")
    parser.add_argument("end_date", nargs="?", default="20261031")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=False,
        help="跳过 stock_near_ma 已有同日同板块结果",
    )
    args = parser.parse_args()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_app(SqlApp)
    # add_app 直接返回引擎实例
    script_engine = main_engine.add_app(ScriptTraderApp)
    assert script_engine is not None

    # CLI 不经 start_strategy，手动挂活跃标志，避免 is_active() 立刻为 False
    cli_name = "__cli_near_ma_duckdb_hist__"
    script_engine._current_script.name = cli_name  # noqa: SLF001
    script_engine.script_active_flags[cli_name] = True

    try:
        run(
            script_engine,
            start_date=args.start_date,
            end_date=args.end_date,
            skip_existing=args.skip_existing,
        )
    finally:
        script_engine.script_active_flags[cli_name] = False
        main_engine.close()


if __name__ == "__main__":
    _cli_main()
