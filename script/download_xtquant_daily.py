"""从指定日期开始补全 xtquant 全市场 A 股日线数据。

建议在交易日收盘后通过 ScriptTrader 运行本脚本。下载结果由 xtquant
写入 MiniQMT 本地行情缓存，不会保存到 VeighNa 数据库。
"""

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from xtquant import xtdata

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


# 新版 xtquant 提供“沪深京A股”板块；旧版将自动回退到后两个板块。
PRIMARY_SECTOR: str = "沪深京A股"
FALLBACK_SECTORS: tuple[str, ...] = ("沪深A股", "京市A股")

# 默认下载起始日期，格式为 YYYYMMDD（包含当天）；可被 run(start_date=...) 覆盖。
# 取 select_near_ma_xtquant.py 中 FIXED_DATES 的最早值，保证默认下载区间覆盖
# 选股均线窗口；改 FIXED_DATES 时记得同步本值。
START_DATE: str = "20220427"

# 每批下载数量。分批后可在两批之间响应 ScriptTrader 的停止操作。
BATCH_SIZE: int = 500

# 板块数据更新频率较低，但更新后才能包含最新上市的股票。
REFRESH_SECTOR_DATA: bool = True


def get_all_stock_codes(engine: "ScriptEngine") -> list[str]:
    """获取当前沪深京 A 股代码，并兼容旧版本的板块分类。"""
    if REFRESH_SECTOR_DATA:
        engine.write_log("正在更新 xtquant 板块分类数据")
        xtdata.download_sector_data()

    stock_codes: list[str] = xtdata.get_stock_list_in_sector(PRIMARY_SECTOR)
    if not stock_codes:
        engine.write_log(
            f"未找到“{PRIMARY_SECTOR}”板块，回退到：{', '.join(FALLBACK_SECTORS)}"
        )
        stock_codes = []
        for sector in FALLBACK_SECTORS:
            stock_codes.extend(xtdata.get_stock_list_in_sector(sector))

    # 多个板块可能有重复代码，同时过滤非沪深京市场的异常成分。
    valid_markets: tuple[str, ...] = (".SH", ".SZ", ".BJ")
    return sorted({code for code in stock_codes if code.endswith(valid_markets)})


def create_progress_callback(
    engine: "ScriptEngine",
    batch_base: int,
    batch_size: int,
    total: int,
) -> Callable[[dict[str, Any]], None]:
    """创建绑定当前批次信息的进度回调。"""

    def on_progress(data: dict[str, Any]) -> None:
        finished: int = int(data.get("finished", 0))
        current: int = min(batch_base + finished, total)
        message: str = str(data.get("message", "")).strip()

        if finished == 1 or finished == batch_size or current % 100 == 0:
            log_text: str = f"下载进度：{current}/{total}"
            if message:
                log_text += f"，{message}"
            engine.write_log(log_text)

    return on_progress


def get_trading_dates(end_date: str, start_date: str = START_DATE) -> list[str]:
    """获取起始日期至结束日期之间的沪市交易日。"""
    try:
        datetime.strptime(start_date, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("start_date 必须是 YYYYMMDD 格式的有效日期") from exc

    if start_date > end_date:
        raise ValueError(f"start_date {start_date} 不能晚于结束日期 {end_date}")

    dates: list[str] = xtdata.get_trading_calendar(
        market="SH",
        start_time=start_date,
        end_time=end_date,
    )
    return [str(date) for date in dates if start_date <= str(date) <= end_date]


def get_stock_open_dates(
    stock_codes: list[str],
    start_date: str = START_DATE,
) -> dict[str, str]:
    """获取股票 IPO 日期；日期缺失时按起始日期处理，避免漏下数据。"""
    open_dates: dict[str, str] = {}

    for code in stock_codes:
        detail: dict[str, Any] | None = xtdata.get_instrument_detail(code)
        open_date: str = str((detail or {}).get("OpenDate", "")).strip()
        if len(open_date) != 8 or not open_date.isdigit():
            open_date = start_date
        open_dates[code] = open_date

    return open_dates


def get_missing_stock_codes(stock_codes: list[str], trade_date: str) -> list[str]:
    """返回指定交易日在 xtquant 本地缓存中没有日线的代码。"""
    missing_codes: list[str] = []

    for start in range(0, len(stock_codes), BATCH_SIZE):
        batch: list[str] = stock_codes[start : start + BATCH_SIZE]
        local_data: dict[str, Any] = xtdata.get_local_data(
            field_list=["time"],
            stock_list=batch,
            period="1d",
            start_time=trade_date,
            end_time=trade_date,
            count=-1,
            dividend_type="none",
            fill_data=False,
        )
        time_frame: Any = local_data.get("time")

        if time_frame is None or time_frame.empty:
            missing_codes.extend(batch)
            continue

        present_codes: set[str] = set(time_frame.dropna(how="all").index)
        missing_codes.extend(code for code in batch if code not in present_codes)

    return missing_codes


def download_stock_codes(
    engine: "ScriptEngine",
    stock_codes: list[str],
    trade_date: str,
) -> bool:
    """下载指定交易日的代码；完整执行返回 True，用户停止时返回 False。"""
    total: int = len(stock_codes)

    for start in range(0, total, BATCH_SIZE):
        if not engine.strategy_active:
            engine.write_log(
                f"下载已停止：{trade_date} 已补充约 {start}/{total} 个标的"
            )
            return False

        batch: list[str] = stock_codes[start : start + BATCH_SIZE]
        on_progress: Callable[[dict[str, Any]], None] = create_progress_callback(
            engine=engine,
            batch_base=start,
            batch_size=len(batch),
            total=total,
        )

        xtdata.download_history_data2(
            stock_list=batch,
            period="1d",
            start_time=trade_date,
            end_time=trade_date,
            callback=on_progress,
        )

    return True


def run(engine: "ScriptEngine", start_date: str = START_DATE) -> None:
    """ScriptTrader 策略入口。

    ``start_date`` 指定下载起始日期（YYYYMMDD，包含当天），默认 ``START_DATE``。
    选股脚本 select_near_ma_xtquant.py 的均线需从更早的固定日期起算，可把起点
    前移（如 "20220427"），或改用薄包装脚本 download_xtquant_for_ma.py 传入。
    """
    end_date: str = datetime.now().strftime("%Y%m%d")
    stock_codes: list[str] = get_all_stock_codes(engine)

    if not stock_codes:
        raise RuntimeError(
            "xtquant 未返回任何 A 股代码，请确认 MiniQMT 已启动且行情服务可用"
        )

    trading_dates: list[str] = get_trading_dates(end_date, start_date)
    if not trading_dates:
        raise RuntimeError(
            f"xtquant 未返回 {start_date} 至 {end_date} 之间的交易日，请检查交易日历"
        )

    engine.write_log(
        f"开始检查 {start_date} 至 {end_date} 的日线数据，"
        f"共 {len(trading_dates)} 个交易日、{len(stock_codes)} 个标的"
    )
    engine.write_log("正在读取标的上市日期")
    open_dates: dict[str, str] = get_stock_open_dates(stock_codes, start_date)

    skipped_dates: int = 0
    downloaded_dates: int = 0
    downloaded_codes: int = 0

    for index, trade_date in enumerate(trading_dates, start=1):
        if not engine.strategy_active:
            engine.write_log(
                f"任务已停止，已检查 {index - 1}/{len(trading_dates)} 个交易日"
            )
            return

        engine.write_log(f"检查交易日 {trade_date}（{index}/{len(trading_dates)}）")
        listed_codes: list[str] = [
            code for code in stock_codes if open_dates[code] <= trade_date
        ]
        missing_codes: list[str] = get_missing_stock_codes(listed_codes, trade_date)

        if not missing_codes:
            skipped_dates += 1
            engine.write_log(f"{trade_date} 本地数据完整，跳过")
            continue

        engine.write_log(
            f"{trade_date} 缺少 {len(missing_codes)}/{len(listed_codes)} 个已上市标的，"
            "开始下载"
        )
        if not download_stock_codes(engine, missing_codes, trade_date):
            return

        downloaded_dates += 1
        downloaded_codes += len(missing_codes)

    engine.write_log(
        f"历史日线补全完成：检查 {len(trading_dates)} 个交易日，"
        f"跳过完整日期 {skipped_dates} 个，下载 {downloaded_dates} 个日期的"
        f" {downloaded_codes} 条标的任务"
    )
