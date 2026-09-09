"""为大 QMT（xtquant-big-convert）提供与 MiniQMT 同名的 ``xtdata``。

选股/下载脚本统一 ``from bigqmt_xtdata import xtdata``，不再 ``from xtquant import xtdata``
（后者指向 MiniQMT 客户端包）。

前置：
1. 大 QMT 已运行并加载 BIGQMT_* 服务端策略；
2. 本机 VeighNa 所用 Python 已 ``pip install -e`` 安装 ``xtquant-big-convert``；
3. 与本文件同目录的 ``bigqmt_signal_trader_client_config.py`` 存在且设置了
   ``BIGQMT_ACCOUNT_ID``（可从 ``bigqmt_signal_trader_client_config.example.py`` 复制）。

历史 K 线经 RPC 读大 QMT 终端本地库；选股先 ``count=1`` 探覆盖，
只对缺当日的标的小批次补数，再一次读全区间。
"""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_CONFIG_PATH = _SCRIPT_DIR / "bigqmt_signal_trader_client_config.py"
_CONFIG_MODULE = "bigqmt_signal_trader_client_config"


def _load_sibling_client_config() -> Any:
    """始终从本 script/ 旁的配置文件加载，避免 sys.modules / 其它路径上的空壳配置。"""
    if not _CONFIG_PATH.is_file():
        raise FileNotFoundError(
            f"缺少 {_CONFIG_PATH.name}。请复制 "
            f"bigqmt_signal_trader_client_config.example.py 为该文件并填写 "
            f"BIGQMT_ACCOUNT_ID（须与大 QMT 端一致）。"
        )

    # 丢掉先前从其它目录 import 进来的同名模块。
    sys.modules.pop(_CONFIG_MODULE, None)

    spec = importlib.util.spec_from_file_location(_CONFIG_MODULE, _CONFIG_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载配置文件：{_CONFIG_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_CONFIG_MODULE] = module
    spec.loader.exec_module(module)
    return module


_config = _load_sibling_client_config()
_ACCOUNT_ID = str(getattr(_config, "BIGQMT_ACCOUNT_ID", "") or "").strip()
if not _ACCOUNT_ID or _ACCOUNT_ID.upper().startswith("YOUR_"):
    raise ValueError(
        f"{_CONFIG_PATH} 未设置有效的 BIGQMT_ACCOUNT_ID（当前={_ACCOUNT_ID!r}）。"
    )

# 仍放入 path，便于 compat 内其它按模块名查找的逻辑。
_script_dir_text = str(_SCRIPT_DIR)
if _script_dir_text in sys.path:
    sys.path.remove(_script_dir_text)
sys.path.insert(0, _script_dir_text)

from bigqmt_signal_trader.xtquant_compat import configure, xtdata as _bq_xtdata

# BigQMT 单次 get_market_data_ex 建议批大小（桥内部也会再切到约 100）。
READ_BATCH_SIZE: int = 100
# 选股全区间读数可能较慢，覆盖默认 30s。
RPC_TIMEOUT_SECONDS: float = float(os.environ.get("BIGQMT_SELECT_RPC_TIMEOUT", "120"))
# 当日缺数补下每批标的数；过大仍可能压垮大 QMT。
DOWNLOAD_TODAY_BATCH_SIZE: int = int(
    os.environ.get("BIGQMT_DOWNLOAD_TODAY_BATCH", "50")
)

configure(account_id=_ACCOUNT_ID, timeout_seconds=RPC_TIMEOUT_SECONDS)
xtdata = _bq_xtdata


def ping(engine: Any | None = None) -> bool:
    """探活大 QMT RPC；失败时写日志并返回 False。"""
    try:
        detail = xtdata.get_instrument_detail("000001.SZ") or {}
        ok = bool(detail)
        if engine is not None:
            name = detail.get("InstrumentName") or detail.get("instrument_name") or ""
            engine.write_log(
                f"大 QMT xtdata 就绪（探测 000001.SZ={'OK ' + name if ok else '空'}；"
                f"account={_ACCOUNT_ID}；config={_CONFIG_PATH})"
            )
        return ok
    except Exception as exc:  # noqa: BLE001
        if engine is not None:
            engine.write_log(
                f"大 QMT xtdata 不可用（account={_ACCOUNT_ID}；"
                f"config={_CONFIG_PATH}）：{exc}"
            )
        return False


def instrument_name(detail: dict[str, Any] | None) -> str:
    """兼容 MiniQMT / BigQMT 两种合约名字段。"""
    if not detail:
        return ""
    return str(
        detail.get("InstrumentName") or detail.get("instrument_name") or ""
    ).strip()


def to_yyyymmdd(value: Any) -> str | None:
    """把交易日/K 线时间规范成 YYYYMMDD。

    MiniQMT ``get_trading_dates`` 多为毫秒时间戳；BigQMT / ContextInfo 常直接
    返回 ``YYYYMMDD`` 或带 ``-`` 的日期字符串。
    """
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return str(value.strftime("%Y%m%d"))
    if isinstance(value, (int, float)):
        ts = float(value)
        # 秒级约 1e9，毫秒约 1e12；小于 1e11 当秒处理。
        if ts >= 1e11:
            ts /= 1000.0
        return datetime.fromtimestamp(ts).strftime("%Y%m%d")
    text = str(value).strip().replace("-", "").replace("/", "").replace(" ", "")
    if len(text) >= 8 and text[:8].isdigit():
        return text[:8]
    if text.isdigit():
        return to_yyyymmdd(int(text))
    return None


def trading_dates_to_yyyymmdd(raw_dates: Any) -> list[str]:
    """规范化 ``get_trading_dates`` 返回值为 YYYYMMDD 列表。"""
    if not raw_dates:
        return []
    result: list[str] = []
    for item in raw_dates:
        day = to_yyyymmdd(item)
        if day:
            result.append(day)
    return result
