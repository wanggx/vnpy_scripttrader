import os
import sys

# 让 script/ 下脚本与旁路 vnpy_xt 的 bigqmt 客户端配置可被 import。
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_VNPY_XT = os.path.abspath(os.path.join(_ROOT, "..", "vnpy_xt"))
for _path in (_ROOT, os.path.join(_ROOT, "script"), _VNPY_XT):
    if os.path.isdir(_path) and _path not in sys.path:
        sys.path.insert(0, _path)

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_ctp import CtpGateway
from vnpy_scripttrader import ScriptTraderApp
from vnpy_sqlapp import SqlApp
# datafeed 保持默认 xt；tushare 经 script/tushare_helper.py 独立实例化按需取数
from vnpy_tushare import Datafeed  # noqa: F401


def main() -> None:
    """Start Trader（选股脚本行情走大 QMT RPC，需 BIGQMT 服务端已运行）"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    main_engine.add_gateway(CtpGateway)
    main_engine.add_app(ScriptTraderApp)
    main_engine.add_app(SqlApp)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
