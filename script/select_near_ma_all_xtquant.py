"""全市场 A 股成本均线选股（沪深京A股，约 5000+ 标的）。

打分规则、均线日期、腰斩过滤、入库表与 ``select_near_ma_xtquant.py`` 相同，
区别只在标的池：本脚本跑全部沪深京 A 股，结果以 ``sector_name=沪深京A股``
写入同一张 ``stock_near_ma``（与申万板块结果互不覆盖）。

当日日线用一次 ``download_history_data2`` 传入全部代码。分批调用会把
MiniQMT 下载会话打挂（无回调、界面假死）；全量一次传入实测约 2.5 分钟可完成。
进度回调字段为 ``finished``（不是 ``done``）。

需 MiniQMT 运行后经 ScriptTrader 执行。历史日线仍由
``download_xtquant_for_ma.py`` 预先下载到本地缓存。调度与行业版相同：
每个交易日 16:00 执行，启动后等下一个 16:00，非交易日跳过。
"""

# pylint: disable=protected-access
from __future__ import annotations

import traceback
from datetime import datetime
from typing import TYPE_CHECKING, Any

from xtquant import xtdata

# 复用行业版的打分/落库/调度，避免两套规则漂移。
import select_near_ma_xtquant as ma
from vnpy_sqlapp import APP_NAME

if TYPE_CHECKING:
    import pandas as pd

    from vnpy_scripttrader.engine import ScriptEngine
    from vnpy_sqlapp import SqlEngine


# 与 download_xtquant_daily.py 一致：新版板块名，旧版回退。
PRIMARY_SECTOR: str = "沪深京A股"
FALLBACK_SECTORS: tuple[str, ...] = ("沪深A股", "京市A股")
# 入库时的 sector_name，与申万行业版共用表、靠主键区分。
SECTOR_NAME: str = PRIMARY_SECTOR
# 合约信息批量读取；get_market_data_ex 仍按此分批（读缓存不会打挂下载通道）。
NAME_BATCH_SIZE: int = 500
READ_BATCH_SIZE: int = 500
# 下载进度日志间隔（callback.finished）。
DOWNLOAD_LOG_EVERY: int = 500


def _get_all_stock_codes(engine: ScriptEngine) -> list[str]:
    """获取当前沪深京 A 股代码，兼容旧版板块分类。"""
    stock_codes: list[str] = xtdata.get_stock_list_in_sector(PRIMARY_SECTOR) or []
    if not stock_codes:
        engine.write_log(
            f"未找到“{PRIMARY_SECTOR}”板块，回退到：{', '.join(FALLBACK_SECTORS)}"
        )
        for sector in FALLBACK_SECTORS:
            stock_codes.extend(xtdata.get_stock_list_in_sector(sector) or [])

    return sorted(
        {code for code in stock_codes if code.endswith(ma.VALID_MARKETS)}
    )


def _filter_universe(
    engine: ScriptEngine,
    stock_codes: list[str],
) -> list[tuple[str, str]] | None:
    """批量读合约信息，排除 ST/*ST 与信息缺失的标的。用户停止时返回 None。"""
    engine.write_log(f"正在读取 {len(stock_codes)} 个标的的合约信息（排除ST）")
    kept: list[tuple[str, str]] = []
    total: int = len(stock_codes)

    for start in range(0, total, NAME_BATCH_SIZE):
        if not engine.is_active():
            engine.write_log(f"筛选已停止：已处理 {start}/{total}")
            return None

        batch: list[str] = stock_codes[start : start + NAME_BATCH_SIZE]
        details: dict[str, Any] = xtdata.get_instrument_detail_list(batch) or {}
        for code in batch:
            detail: dict[str, Any] | None = details.get(code)
            if not detail:
                continue
            name: str = str(detail.get("InstrumentName", "")).strip()
            if ma.EXCLUDE_ST and "ST" in name.upper():
                continue
            kept.append((code, name))

        engine.write_log(f"筛选进度：{min(start + len(batch), total)}/{total}")

    return kept


def _download_today(
    engine: ScriptEngine,
    universe: list[tuple[str, str]],
    trade_date: str,
) -> bool:
    """一次传入全部代码，增量补当日日线。

    不要按 200/500 循环调用 ``download_history_data2``：连续分批会把 MiniQMT
    下载通道打挂（callback 不再触发，调用永不返回）。全市场一次传入可跑完。
    本次调用期间无法响应停止（xtquant 同步阻塞）；开始前检查一次 is_active。
    失败只记日志，后续读缓存兜底。
    """
    if not engine.is_active():
        engine.write_log("当日下载已停止（尚未开始）")
        return False

    codes: list[str] = [code for code, _ in universe]
    total: int = len(codes)
    engine.write_log(
        f"开始增量下载当日日线（{trade_date}）：{total} 个标的（一次全量，勿分批）"
    )
    last_logged: list[int] = [0]

    def on_progress(data: dict[str, Any]) -> None:
        # xtquant 实际字段是 finished；兼容误用 done 的旧文档。
        finished: int = int(data.get("finished") or data.get("done") or 0)
        total_n: int = int(data.get("total") or total)
        if (
            finished == 1
            or finished >= total_n
            or finished - last_logged[0] >= DOWNLOAD_LOG_EVERY
        ):
            last_logged[0] = finished
            msg: str = str(data.get("message", "")).strip()
            extra: str = f"，{msg}" if msg else ""
            engine.write_log(f"当日下载进度：{finished}/{total_n}{extra}")

    try:
        xtdata.download_history_data2(
            stock_list=codes,
            period="1d",
            start_time=trade_date,
            end_time=trade_date,
            callback=on_progress,
            incrementally=True,
        )
    except Exception as exc:  # noqa: BLE001 - 下载失败不中断，读缓存兜底
        engine.write_log(f"当日日线下载异常，将改用本地缓存：{exc}")
        return True

    engine.write_log(f"当日日线下载完成：{total} 个标的")
    return True


def _load_bar_series(
    engine: ScriptEngine,
    universe: list[tuple[str, str]],
    start_time: str,
    end_time: str,
) -> dict[str, pd.DataFrame] | None:
    """先一次补当日日线，再分批读全区间前复权 close/high。"""
    if not _download_today(engine, universe, end_time):
        return None

    codes: list[str] = [code for code, _ in universe]
    result: dict[str, pd.DataFrame] = {}
    total: int = len(codes)
    logged_sample: bool = False

    for start in range(0, total, READ_BATCH_SIZE):
        if not engine.is_active():
            engine.write_log(f"读取已停止：已补充约 {start}/{total} 个标的")
            return None

        batch: list[str] = codes[start : start + READ_BATCH_SIZE]
        data: dict[str, pd.DataFrame] = xtdata.get_market_data_ex(
            field_list=["close", "high"],
            stock_list=batch,
            period="1d",
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type=ma.DIVIDEND_TYPE,
            fill_data=False,
        )
        for code, df in data.items():
            if df is None or len(df) == 0:
                continue
            result[code] = ma._normalize_bars(df)

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


def _run_once(engine: ScriptEngine) -> None:
    """对沪深京 A 股全市场执行一次选股。"""
    sql_engine: SqlEngine | None = engine.main_engine.get_engine(APP_NAME)
    if sql_engine is None:
        raise RuntimeError(
            "选股脚本依赖 SqlApp，请先加载 SqlApp（script/run.py 中 add_app(SqlApp)）"
        )
    driver: str = getattr(sql_engine.database, "driver_name", "sqlite")
    engine.write_log(f"SqlApp 已就绪，数据库驱动：{driver}")

    try:
        engine.write_log("正在更新 xtquant 板块分类数据")
        xtdata.download_sector_data()
    except Exception as exc:  # noqa: BLE001 - 板块更新失败则本轮结束
        engine.write_log(f"更新 xtquant 板块分类数据失败：{exc}")
        return

    stock_codes: list[str] = _get_all_stock_codes(engine)
    if not stock_codes:
        engine.write_log("xtquant 未返回任何 A 股代码，本轮结束")
        return
    engine.write_log(f"全市场标的池“{SECTOR_NAME}”：{len(stock_codes)} 个")

    universe: list[tuple[str, str]] | None = _filter_universe(engine, stock_codes)
    if universe is None:
        engine.write_log("选股已停止（筛选阶段）")
        return
    if not universe:
        engine.write_log("筛选后无可用标的，结束")
        return
    engine.write_log(f"筛选后标的 {len(universe)} 个")

    end_date: str = datetime.now().strftime("%Y%m%d")
    calendar: list[str] = ma._get_trading_dates(ma.FIXED_DATES[0], end_date)
    if not calendar:
        raise RuntimeError(
            f"xtquant 未返回 {ma.FIXED_DATES[0]} 至 {end_date} 的交易日，请检查 xtquant 连接"
        )
    window_start: dict[str, str] = {
        fixed_date: min(d for d in calendar if d >= fixed_date)
        for fixed_date in ma.FIXED_DATES
    }
    start_min: str = min(window_start.values())
    engine.write_log(f"交易日历 {len(calendar)} 个，均线窗口起点：{window_start}")

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

    trade_date: str = ma._decide_trade_date(engine, series_map, calendar)
    engine.write_log(f"本次计算交易日 T = {trade_date}")

    name_map: dict[str, str] = dict(universe)
    scored: list[dict[str, Any]] = []
    skipped_few_bars: int = 0
    skipped_not_halved: int = 0
    total: int = len(series_map)
    for index, (code, bars) in enumerate(series_map.items(), start=1):
        if index % 1000 == 0:
            engine.write_log(f"打分进度：{index}/{total}")
            if not engine.is_active():
                engine.write_log(f"选股已停止（打分阶段，已处理 {index}/{total}）")
                return
        if bars["close"].dropna().size < ma.MIN_BARS:
            skipped_few_bars += 1
            continue
        info: dict[str, Any] | None = ma._score_symbol(
            bars["close"], bars["high"], trade_date, window_start
        )
        if info is None:
            if ma.REQUIRE_HALVED and ma._is_halved(
                bars["close"], bars["high"], trade_date
            ):
                continue
            skipped_not_halved += 1
            continue
        info["code"] = code
        info["name"] = name_map.get(code, "")
        info["sector_name"] = SECTOR_NAME
        scored.append(info)

    engine.write_log(
        f"打分完成：{len(scored)} 个标的接近至少一条均线，"
        f"因日线数据不足 {ma.MIN_BARS} 条剔除 {skipped_few_bars} 个，"
        f"因未腰斩剔除 {skipped_not_halved} 个"
    )

    scored.sort(key=lambda x: (-x["score"], x["code"]))
    top: list[dict[str, Any]] = scored[: ma.TOP_N]
    if not top:
        engine.write_log("无标的接近任何均线，结束")
        return
    engine.write_log(
        f"取前 {len(top)} 个入库，最高分 {top[0]['score']}，最低分 {top[-1]['score']}"
    )

    ma._save_results(sql_engine, driver, trade_date, SECTOR_NAME, top, engine)
    engine.write_log(f"全市场“{SECTOR_NAME}”选股完成")


def run(engine: ScriptEngine) -> None:
    """ScriptTrader 策略入口：每个交易日 16:00 对全市场 A 股执行选股。"""
    engine.write_log(
        f"全市场选股调度启动：每交易日 {ma.RUN_HOUR:02d}:{ma.RUN_MINUTE:02d} 执行，"
        f"标的池={SECTOR_NAME}，非交易日跳过，等待首个触发点..."
    )
    while engine.is_active():
        wake_at: datetime = ma._next_run_dt(datetime.now())
        engine.write_log(f"下次执行时间：{wake_at.strftime('%Y-%m-%d %H:%M:%S')}")
        if not ma._sleep_until(engine, wake_at):
            break

        today: datetime = datetime.now()
        if not ma._is_trading_day(today):
            engine.write_log(f"{today.strftime('%Y-%m-%d')} 非交易日，跳过")
            continue

        try:
            _run_once(engine)
        except Exception:  # noqa: BLE001 - 单轮失败不中断调度
            engine.write_log(f"本轮执行异常：\n{traceback.format_exc()}")
    engine.write_log("全市场选股调度已停止")
