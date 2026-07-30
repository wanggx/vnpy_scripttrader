from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_ctp import CtpGateway
from vnpy_scripttrader import ScriptTraderApp
from vnpy_sqlapp import SqlApp
# datafeed 保持默认 xt；tushare 经 script/tushare_helper.py 独立实例化按需取数，
# 此处导入仅为启动时校验 vnpy_tushare 可用，token 配在 vt_setting.json 的 tushare.token
from vnpy_tushare import Datafeed  # noqa: F401


def main() -> None:
    """Start Trader"""
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
