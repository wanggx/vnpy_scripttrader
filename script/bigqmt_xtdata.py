"""为大 QMT（xtquant-big-convert）提供与 MiniQMT 同名的 ``xtdata``。

选股/下载脚本统一 ``from bigqmt_xtdata import xtdata``，不再 ``from xtquant import xtdata``
（后者指向 MiniQMT 客户端包）。

前置：
1. 大 QMT 已运行并加载 BIGQMT_* 服务端策略；
2. ``bigqmt_signal_trader_client_config.py`` 可被 import（与 QMT 端账号/Redis 一致）；
3. 已安装 ``xtquant-big-convert`` 或本机 ``vnpy_xt`` 自带的 bigqmt_signal_trader。

历史 K 线读的是大 QMT 终端本地库；缺周期请在终端「数据管理」补，
``download_history_data2`` 在大 QMT 上经常不可用，脚本侧只作尽力而为。
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

# 让同目录的 client_config、以及旁路的 vnpy_xt 配置可被 import。
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
_CANDIDATE_ROOTS = (
    _REPO_ROOT,
    _SCRIPT_DIR,
    Path(r"D:\kproject\QT\vnpy_xt"),
    Path(r"D:\kproject\QT\xtquant_big_convert"),
    Path(r"D:\kproject\QT\xtquant_big_convert\src"),
)
for _root in _CANDIDATE_ROOTS:
    text = str(_root)
    if _root.is_dir() and text not in sys.path:
        sys.path.insert(0, text)

# BigQMT 单次 get_market_data_ex 建议批大小（桥内部也会再切到约 100）。
READ_BATCH_SIZE: int = 100
# 选股全区间读数可能较慢，覆盖默认 30s。
RPC_TIMEOUT_SECONDS: float = float(os.environ.get("BIGQMT_SELECT_RPC_TIMEOUT", "120"))


def _ensure_configured() -> Any:
    from bigqmt_signal_trader.xtquant_compat import configure, xtdata

    configure(timeout_seconds=RPC_TIMEOUT_SECONDS)
    return xtdata


xtdata = _ensure_configured()


def ping(engine: Any | None = None) -> bool:
    """探活大 QMT RPC；失败时写日志并返回 False。"""
    try:
        detail = xtdata.get_instrument_detail("000001.SZ") or {}
        ok = bool(detail)
        if engine is not None:
            name = detail.get("InstrumentName") or detail.get("instrument_name") or ""
            engine.write_log(
                f"大 QMT xtdata 就绪（探测 000001.SZ={'OK ' + name if ok else '空'}）"
            )
        return ok
    except Exception as exc:  # noqa: BLE001
        if engine is not None:
            engine.write_log(f"大 QMT xtdata 不可用：{exc}")
        return False


def instrument_name(detail: dict[str, Any] | None) -> str:
    """兼容 MiniQMT / BigQMT 两种合约名字段。"""
    if not detail:
        return ""
    return str(
        detail.get("InstrumentName") or detail.get("instrument_name") or ""
    ).strip()
