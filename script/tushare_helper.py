"""vnpy_tushare 独立数据访问 helper。

平台默认 datafeed 仍为 xt（由 ``SETTINGS["datafeed.name"]`` 决定，本模块不改）；
在此前提下单独实例化 ``TushareDatafeed`` 用于按需获取 tushare 数据，绕开全局
``get_datafeed()`` 单例。tushare token 从独立配置键 ``SETTINGS["tushare.token"]``
读取，避免与 ``datafeed.password`` 共享而影响 xt（xt 的 client 模式不读 password，
但独立键更稳妥、零耦合）。

使用前在 vt_setting.json 中新增::

    "tushare.token": "<你的 tushare token>"

而后任意 ScriptTrader 脚本均可::

    from tushare_helper import query_bars, get_a_share_list
"""

from datetime import datetime

from tushare.pro.client import DataApi

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.setting import SETTINGS
from vnpy_tushare import Datafeed as TushareDatafeed
from vnpy_tushare.tushare_datafeed import to_ts_asset


# tushare 的 username 仅为占位（init 要求非空），token 才是真正的凭证
TUSHARE_USERNAME: str = "tushare"

# tushare ts_code 后缀 → vnpy Exchange
TS_SUFFIX_TO_EXCHANGE: dict[str, Exchange] = {
    ".SH": Exchange.SSE,
    ".SZ": Exchange.SZSE,
    ".BJ": Exchange.BSE,
}

_datafeed: TushareDatafeed | None = None


def get_tushare_datafeed() -> TushareDatafeed:
    """返回已初始化的 TushareDatafeed 单例（独立于平台 datafeed）。"""
    global _datafeed
    if _datafeed is not None and _datafeed.inited:
        return _datafeed

    token: str = SETTINGS.get("tushare.token", "")
    if not token:
        raise RuntimeError(
            "未配置 tushare token，请在 vt_setting.json 中设置 tushare.token"
        )

    df: TushareDatafeed = TushareDatafeed()
    # 覆盖凭证，绕开 datafeed.username/password 共享键
    df.username = TUSHARE_USERNAME
    df.password = token
    if not df.init():
        raise RuntimeError("tushare 数据服务初始化失败，请检查 token 是否有效")

    _datafeed = df
    return df


def get_tushare_pro() -> DataApi:
    """返回已初始化的 tushare pro DataApi，用于 stock_basic 等非 K 线接口。"""
    return get_tushare_datafeed().pro


def query_bars(
    symbol: str,
    exchange: Exchange,
    interval: Interval,
    start: datetime,
    end: datetime,
) -> list[BarData]:
    """按需查询 tushare 历史 K 线，返回 vnpy BarData 列表。"""
    req: HistoryRequest = HistoryRequest(
        symbol=symbol,
        exchange=exchange,
        start=start,
        end=end,
        interval=interval,
    )
    bars: list[BarData] | None = get_tushare_datafeed().query_bar_history(req)
    return bars or []


def ts_code_to_vt(ts_code: str) -> tuple[str, Exchange]:
    """将 tushare ts_code（如 000001.SZ）转为 (symbol, Exchange)。"""
    for suffix, exchange in TS_SUFFIX_TO_EXCHANGE.items():
        if ts_code.endswith(suffix):
            return ts_code[: -len(suffix)], exchange
    raise ValueError(f"无法识别的 tushare 代码：{ts_code}")


def get_a_share_list() -> list[tuple[str, Exchange]]:
    """获取当前在市的沪深京 A 股 (symbol, Exchange) 列表。

    复用 datafeed 自带的 ``to_ts_asset`` 判定 A 股权益（asset == "E"），
    自动排除 B 股、ETF、指数，与 xt 脚本“沪深京A股”板块口径一致。
    """
    pro: DataApi = get_tushare_pro()
    df = pro.stock_basic(list_status="L")

    codes: list[tuple[str, Exchange]] = []
    for ts_code in df["ts_code"].tolist():
        try:
            symbol, exchange = ts_code_to_vt(ts_code)
        except ValueError:
            continue
        if to_ts_asset(symbol, exchange) == "E":
            codes.append((symbol, exchange))
    return codes
