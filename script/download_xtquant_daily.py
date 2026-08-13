"""从指定日期开始补全 xtquant 全市场 A 股日线数据。

建议在交易日收盘后通过 ScriptTrader 运行本脚本。下载结果由 xtquant
写入 MiniQMT 本地行情缓存，不会保存到 VeighNa 数据库。

策略（避免按天扫描、避免分批 ``download_history_data2`` 把 MiniQMT 打挂）：

1. 按标的读本地最后一根日线，已覆盖最新交易日的跳过；
2. 其余代码一次传入，``start_time=start_date``、``end_time=今天``、
   ``incrementally=True`` 只补缺口；
3. 全区间一次调用失败时，再按自然年切几刀重试（每年仍是一次全名单，
   不按 500 一批循环）。
"""

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

# 读取本地最后一根时的分批大小（只读缓存，不会打挂下载通道）。
READ_BATCH_SIZE: int = 500

# 与选股脚本一致：缓存存的是未复权 K 线，读取时按前复权检查是否已覆盖。
DIVIDEND_TYPE: str = "front"

# 下载进度日志间隔（callback.finished，按标的计数）。
DOWNLOAD_LOG_EVERY: int = 200

# 板块数据更新频率较低，但更新后才能包含最新上市的股票。
REFRESH_SECTOR_DATA: bool = True

VALID_MARKETS: tuple[str, ...] = (".SH", ".SZ", ".BJ")


def get_all_stock_codes(engine: "ScriptEngine") -> list[str]:
    """获取当前沪深京 A 股代码，并兼容旧版本的板块分类。"""
    if REFRESH_SECTOR_DATA:
        engine.write_log("正在更新 xtquant 板块分类数据")
        xtdata.download_sector_data()

    stock_codes: list[str] = xtdata.get_stock_list_in_sector(PRIMARY_SECTOR) or []
    if not stock_codes:
        engine.write_log(
            f"未找到“{PRIMARY_SECTOR}”板块，回退到：{', '.join(FALLBACK_SECTORS)}"
        )
        stock_codes = []
        for sector in FALLBACK_SECTORS:
            stock_codes.extend(xtdata.get_stock_list_in_sector(sector) or [])

    return sorted({code for code in stock_codes if code.endswith(VALID_MARKETS)})


def get_trading_dates(end_date: str, start_date: str = START_DATE) -> list[str]:
    """获取起始日期至结束日期之间的沪市交易日。

    使用 ``get_trading_dates`` 而非 ``get_trading_calendar``，避免部分客户端
    因 ``download_holiday_data`` 未实现而抛 ``function not realize``。
    """
    try:
        datetime.strptime(start_date, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("start_date 必须是 YYYYMMDD 格式的有效日期") from exc

    if start_date > end_date:
        raise ValueError(f"start_date {start_date} 不能晚于结束日期 {end_date}")

    timestamps: list[int] = xtdata.get_trading_dates(
        market="SH", start_time=start_date, end_time=end_date, count=-1
    )
    dates: list[str] = [
        datetime.fromtimestamp(ts / 1000).strftime("%Y%m%d") for ts in timestamps
    ]
    return [d for d in dates if start_date <= d <= end_date]


def _index_to_yyyymmdd(idx: Any) -> str | None:
    """把 get_market_data_ex 的单根 index 转成 YYYYMMDD。"""
    if idx is None:
        return None
    if hasattr(idx, "strftime"):
        return str(idx.strftime("%Y%m%d"))
    if isinstance(idx, (int, float)):
        return datetime.fromtimestamp(int(idx) / 1000).strftime("%Y%m%d")
    text: str = str(idx).replace("-", "").replace(" ", "")
    if len(text) >= 8 and text[:8].isdigit():
        return text[:8]
    return None


def get_last_bar_dates(
    engine: "ScriptEngine",
    stock_codes: list[str],
    end_date: str,
) -> dict[str, str] | None:
    """读取各标的本地缓存中最后一根前复权日线日期。用户停止时返回 None。

    MiniQMT 本地只存未复权 K 线；``dividend_type=front`` 在读取时复权，
    与选股脚本口径一致。有 K 线但复权因子缺失时会视为未覆盖，从而再补下载。
    """
    last_dates: dict[str, str] = {}
    total: int = len(stock_codes)

    for start in range(0, total, READ_BATCH_SIZE):
        if not engine.is_active():
            engine.write_log(f"读取本地日期已停止：已处理 {start}/{total}")
            return None

        batch: list[str] = stock_codes[start : start + READ_BATCH_SIZE]
        data: dict[str, Any] = xtdata.get_market_data_ex(
            field_list=["close"],
            stock_list=batch,
            period="1d",
            start_time="",
            end_time=end_date,
            count=1,
            dividend_type=DIVIDEND_TYPE,
            fill_data=False,
        )
        for code in batch:
            df = data.get(code)
            if df is None or len(df) == 0:
                continue
            bar_date: str | None = _index_to_yyyymmdd(df.index[-1])
            if bar_date:
                last_dates[code] = bar_date

        engine.write_log(
            f"本地最后日期进度：{min(start + len(batch), total)}/{total}"
        )

    return last_dates


def _year_ranges(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """把闭区间按自然年切开，每年仍一次调用。"""
    ranges: list[tuple[str, str]] = []
    year: int = int(start_date[:4])
    end_year: int = int(end_date[:4])
    while year <= end_year:
        lo: str = start_date if year == int(start_date[:4]) else f"{year}0101"
        hi: str = end_date if year == end_year else f"{year}1231"
        ranges.append((lo, hi))
        year += 1
    return ranges


def download_history(
    engine: "ScriptEngine",
    stock_codes: list[str],
    start_time: str,
    end_time: str,
    label: str,
) -> bool:
    """一次传入全部代码下载区间日线。开始前检查停止；调用期间无法中断。

    返回是否调用成功（异常为 False）。用户在调用前已停止则返回 False。
    """
    if not engine.is_active():
        engine.write_log(f"{label}已停止（尚未开始）")
        return False

    total: int = len(stock_codes)
    engine.write_log(
        f"{label}：{total} 个标的，{start_time} 至 {end_time}，一次全量 incrementally"
    )
    last_logged: list[int] = [0]

    def on_progress(data: dict[str, Any]) -> None:
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
            engine.write_log(f"{label}进度：{finished}/{total_n}{extra}")

    xtdata.download_history_data2(
        stock_list=stock_codes,
        period="1d",
        start_time=start_time,
        end_time=end_time,
        callback=on_progress,
        incrementally=True,
    )
    engine.write_log(f"{label}完成")
    return True


def run(engine: "ScriptEngine", start_date: str = START_DATE) -> None:
    """ScriptTrader 策略入口。

    ``start_date`` 指定下载起始日期（YYYYMMDD，包含当天），默认 ``START_DATE``。
    选股脚本的均线需从更早的固定日期起算，可把起点前移，或改用
    ``download_xtquant_for_ma.py`` 传入。
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
    latest: str = trading_dates[-1]

    engine.write_log(
        f"开始检查 {start_date} 至 {end_date} 的日线数据，"
        f"共 {len(trading_dates)} 个交易日、{len(stock_codes)} 个标的，"
        f"最新交易日 {latest}"
    )

    last_dates: dict[str, str] | None = get_last_bar_dates(
        engine, stock_codes, end_date
    )
    if last_dates is None:
        return

    need_codes: list[str] = [
        code for code in stock_codes if last_dates.get(code, "") < latest
    ]
    complete: int = len(stock_codes) - len(need_codes)
    engine.write_log(
        f"本地已覆盖最新交易日 {complete} 个，需补齐 {len(need_codes)} 个"
    )
    if not need_codes:
        engine.write_log("本地日线已完整，无需下载")
        return

    try:
        if download_history(
            engine, need_codes, start_date, end_date, "全区间下载"
        ):
            engine.write_log("历史日线补全完成")
        return
    except Exception as exc:  # noqa: BLE001 - 全区间失败则按年重试
        engine.write_log(f"全区间一次下载失败，改为按年补齐：{exc}")

    for lo, hi in _year_ranges(start_date, end_date):
        if not engine.is_active():
            engine.write_log("按年下载已停止")
            return
        try:
            download_history(engine, need_codes, lo, hi, f"{lo[:4]}年下载")
        except Exception as year_exc:  # noqa: BLE001 - 单年失败继续下一年
            engine.write_log(f"{lo[:4]}年下载异常：{year_exc}")

    engine.write_log("历史日线补全完成")
