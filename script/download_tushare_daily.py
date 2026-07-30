"""从 tushare 补全 A 股日线数据并写入 VeighNa 数据库。

与 ``download_xtquant_daily.py`` 的区别：本脚本通过 vnpy_tushare 拉取数据并落库
（platform datafeed 仍为 xt，互不影响）；xt 那个脚本走 xtquant 本地缓存、不入库。
tushare 有调用频率限制，按 ``REQUEST_SLEEP`` 间隔请求；单只标的失败不影响其余。

建议在交易日收盘后通过 ScriptTrader 运行，避免把当日未走完的日线写入库。
"""

import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import DB_TZ, get_database

from tushare_helper import get_a_share_list, query_bars

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


# 默认下载起始日期，格式 YYYYMMDD（包含当天）；可被 run(start_date=...) 覆盖。
# 与 download_xtquant_daily.py 保持一致，覆盖 select_near_ma_xtquant.py 的均线窗口。
START_DATE: str = "20220427"

# tushare 接口调用间隔（秒），用于规避每分钟频率限制；按账户积分自行调整。
REQUEST_SLEEP: float = 0.2

# 命中频率限制时，等待重试的时间（秒）。
RETRY_WAIT: float = 60.0


def build_last_end_index() -> dict[tuple[str, str], datetime]:
    """读取数据库日线概览，返回 {(symbol, exchange): 最后日线 datetime}。"""
    database = get_database()
    last_end: dict[tuple[str, str], datetime] = {}
    for overview in database.get_bar_overview():
        if overview.interval != Interval.DAILY or overview.end is None:
            continue
        key: tuple[str, str] = (overview.symbol, overview.exchange.value)
        last_end[key] = overview.end
    return last_end


def download_one(
    symbol: str,
    exchange: Exchange,
    fetch_start: datetime,
    end_dt: datetime,
) -> int:
    """下载单只标的并入库，返回入库条数；频率受限时等待后重试一次。"""
    try:
        bars = query_bars(symbol, exchange, Interval.DAILY, fetch_start, end_dt)
    except Exception:
        time.sleep(RETRY_WAIT)
        bars = query_bars(symbol, exchange, Interval.DAILY, fetch_start, end_dt)

    if not bars:
        return 0

    get_database().save_bar_data(bars)
    return len(bars)


def run(engine: "ScriptEngine", start_date: str = START_DATE) -> None:
    """ScriptTrader 策略入口。

    ``start_date`` 指定下载起始日期（YYYYMMDD，包含当天），默认 ``START_DATE``。
    已入库的标的会从其最后一条日线之后续传，未入库的从 ``start_date`` 起全量拉取。
    """
    start_dt: datetime = datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=DB_TZ)
    end_dt: datetime = datetime.now(DB_TZ)

    if start_dt >= end_dt:
        raise ValueError(f"start_date {start_date} 不能晚于当前时间")

    engine.write_log("正在获取 A 股代码列表（tushare stock_basic）")
    stock_list: list[tuple[str, Exchange]] = get_a_share_list()
    if not stock_list:
        raise RuntimeError("tushare 未返回任何 A 股代码，请检查 token 权限")

    engine.write_log(
        f"开始检查 {start_date} 至今的日线数据，共 {len(stock_list)} 只标的"
    )
    last_end: dict[tuple[str, str], datetime] = build_last_end_index()
    engine.write_log(f"数据库已有 {len(last_end)} 条日线概览，将按此增量续传")

    total: int = len(stock_list)
    downloaded: int = 0
    skipped: int = 0
    failed: int = 0

    for index, (symbol, exchange) in enumerate(stock_list, start=1):
        if not engine.strategy_active:
            engine.write_log(f"任务已停止，已处理 {index - 1}/{total}")
            return

        existing_end: datetime | None = last_end.get((symbol, exchange.value))
        fetch_start: datetime = (
            existing_end + timedelta(days=1) if existing_end else start_dt
        )
        if fetch_start >= end_dt:
            skipped += 1
            continue

        try:
            count: int = download_one(symbol, exchange, fetch_start, end_dt)
        except Exception as ex:
            failed += 1
            engine.write_log(f"{symbol}.{exchange.value} 拉取失败：{ex}")
            time.sleep(REQUEST_SLEEP)
            continue

        if count:
            downloaded += 1
            if index % 50 == 0 or index == total:
                engine.write_log(
                    f"进度 {index}/{total}，{symbol}.{exchange.value} 入库 {count} 条"
                )
        else:
            skipped += 1

        time.sleep(REQUEST_SLEEP)

    engine.write_log(
        f"tushare 日线补全完成：共 {total} 只，下载 {downloaded}，"
        f"跳过 {skipped}，失败 {failed}"
    )
