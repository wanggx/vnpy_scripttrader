from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_ctp import CtpGateway
from vnpy_scripttrader import ScriptTraderApp
from vnpy_sqlapp import SqlApp


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
