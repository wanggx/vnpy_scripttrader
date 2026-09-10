"""从指定日期开始补全大 QMT 终端本地日线（经 BigQMT RPC）。

建议在交易日收盘后通过 ScriptTrader 运行。下载走大 QMT，**不会**写入
VeighNa 数据库；大 QMT 上 ``download_history_data2`` 常不可用，失败时请改在
终端「数据管理」手工补。

策略：
1. 按标的 ``get_market_data_ex(count=1)`` 看本地最后一根，已覆盖最新交易日的跳过；
2. 缺的分批（``DOWNLOAD_TODAY_BATCH_SIZE``）传入，``incrementally=True`` 拉
   ``start_date→今天``；一次性回填传 ``force_full=True`` 跳过第 1 步；
3. 全区间失败再按自然年切刀重试。
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any

import bigqmt_xtdata
from bigqmt_xtdata import xtdata

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


PRIMARY_SECTOR: str = "沪深京A股"
FALLBACK_SECTORS: tuple[str, ...] = ("沪深A股", "京市A股")
START_DATE: str = "20220427"
READ_BATCH_SIZE: int = bigqmt_xtdata.READ_BATCH_SIZE
DIVIDEND_TYPE: str = "front"
REFRESH_SECTOR_DATA: bool = True
VALID_MARKETS: tuple[str, ...] = (".SH", ".SZ", ".BJ")


def get_all_stock_codes(engine: "ScriptEngine") -> list[str]:
    """获取当前沪深京 A 股代码，并兼容旧版本的板块分类。"""
    if REFRESH_SECTOR_DATA:
        engine.write_log("正在更新大 QMT 板块分类数据")
        try:
            xtdata.download_sector_data()
        except Exception as exc:  # noqa: BLE001
            engine.write_log(f"更新板块分类失败（继续）：{exc}")

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
    """获取起始日期至结束日期之间的沪市交易日。"""
    try:
        datetime.strptime(start_date, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("start_date 必须是 YYYYMMDD 格式的有效日期") from exc

    if start_date > end_date:
        raise ValueError(f"start_date {start_date} 不能晚于结束日期 {end_date}")

    raw = xtdata.get_trading_dates(
        market="SH", start_time=start_date, end_time=end_date, count=-1
    )
    dates: list[str] = bigqmt_xtdata.trading_dates_to_yyyymmdd(raw)
    return [d for d in dates if start_date <= d <= end_date]


def _index_to_yyyymmdd(idx: Any) -> str | None:
    """把 get_market_data_ex 的单根 index 转成 YYYYMMDD。"""
    return bigqmt_xtdata.to_yyyymmdd(idx)


def get_last_bar_dates(
    engine: "ScriptEngine",
    stock_codes: list[str],
    end_date: str,
) -> dict[str, str] | None:
    """读取各标的最后一根前复权日线日期。用户停止时返回 None。"""
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
    """把闭区间按自然年切开。"""
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
    """分批下载区间日线，禁止一次塞入全市场。

    按 ``DOWNLOAD_TODAY_BATCH_SIZE`` 切批，每批 ``download_history_data2(
    incrementally=True)`` 只补缺口、不重复下已存在数据。单批失败记日志、
    跳过该批继续。用户停止返回 False。与 ``select_near_ma_xtquant`` 当日
    补数的分批口径一致。
    """
    if not engine.is_active():
        engine.write_log(f"{label}已停止（尚未开始）")
        return False

    total: int = len(stock_codes)
    batch_size: int = max(1, bigqmt_xtdata.DOWNLOAD_TODAY_BATCH_SIZE)
    engine.write_log(
        f"{label}：{total} 个标的，{start_time} 至 {end_time}，"
        f"分批 incrementally（每批 {batch_size}）"
    )

    for start in range(0, total, batch_size):
        if not engine.is_active():
            engine.write_log(f"{label}已停止：{start}/{total}")
            return False
        batch: list[str] = stock_codes[start : start + batch_size]
        try:
            xtdata.download_history_data2(
                stock_list=batch,
                period="1d",
                start_time=start_time,
                end_time=end_time,
                incrementally=True,
            )
        except Exception as exc:  # noqa: BLE001 - 单批失败跳过，不阻断
            engine.write_log(
                f"{label}批次失败 {start + 1}-{start + len(batch)}/{total}：{exc}"
            )
            continue
        engine.write_log(
            f"{label}进度：{min(start + len(batch), total)}/{total}"
        )

    engine.write_log(f"{label}完成")
    return True


def run(
    engine: "ScriptEngine",
    start_date: str = START_DATE,
    force_full: bool = False,
) -> None:
    """ScriptTrader 策略入口。

    force_full=True 时跳过「最新交易日是否已覆盖」的过滤，直接对全部标的
    按 ``[start_date, 今天]`` 下载（``incrementally=True`` 只补缺口、不重复
    下载已存在数据）。一次性历史回填应传 True：否则只要某标的偶然有过最新
    一根，就会被判为「已完整」而漏掉更早的历史缺口。
    """
    if not bigqmt_xtdata.ping(engine):
        engine.write_log("大 QMT RPC 不可用，下载结束")
        return

    end_date: str = datetime.now().strftime("%Y%m%d")
    stock_codes: list[str] = get_all_stock_codes(engine)

    if not stock_codes:
        raise RuntimeError(
            "大 QMT 未返回任何 A 股代码，请确认 BIGQMT 服务端已运行"
        )

    trading_dates: list[str] = get_trading_dates(end_date, start_date)
    if not trading_dates:
        raise RuntimeError(
            f"大 QMT 未返回 {start_date} 至 {end_date} 之间的交易日"
        )
    latest: str = trading_dates[-1]

    if force_full:
        need_codes: list[str] = stock_codes
        engine.write_log(
            f"force_full：跳过本地最新根检查，直接对全部 {len(need_codes)} 个标的"
            f"按 {start_date} 至 {end_date} 补缺口（最新交易日 {latest}）"
        )
    else:
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

        need_codes = [
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
    except Exception as exc:  # noqa: BLE001
        engine.write_log(f"全区间一次下载失败，改为按年补齐：{exc}")

    for lo, hi in _year_ranges(start_date, end_date):
        if not engine.is_active():
            engine.write_log("按年下载已停止")
            return
        try:
            download_history(engine, need_codes, lo, hi, f"{lo[:4]}年下载")
        except Exception as year_exc:  # noqa: BLE001
            engine.write_log(f"{lo[:4]}年下载异常：{year_exc}")

    engine.write_log(
        "历史日线补全流程结束。"
        "若仍缺数，请在大 QMT「数据管理」手工下载日线后再跑选股。"
    )
