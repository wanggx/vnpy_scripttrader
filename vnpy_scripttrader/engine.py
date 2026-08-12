import sys
import inspect
import importlib
import traceback
import threading
from types import ModuleType
from typing import Any
from collections.abc import Callable
from collections.abc import Sequence
from pathlib import Path
from datetime import datetime
from threading import Thread

from pandas import DataFrame

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine, LogEngine
from vnpy.trader.constant import Direction, Offset, OrderType, Interval
from vnpy.trader.object import (
    BaseData,
    OrderRequest,
    HistoryRequest,
    SubscribeRequest,
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    LogData,
    BarData,
    CancelRequest
)
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed
from vnpy.trader.utility import load_json, save_json

from .base import APP_NAME, EVENT_SCRIPT_LOG, EVENT_SCRIPT_STRATEGY, ScriptData


class ScriptEngine(BaseEngine):
    """"""
    setting_filename: str = "script_trader_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        # 多脚本记录状态（替代原 strategy_active / strategy_thread 单脚本字段）
        self.scripts: dict[str, ScriptData] = {}            # script_name -> ScriptData
        self.script_threads: dict[str, Thread] = {}         # script_name -> Thread
        self.script_active_flags: dict[str, bool] = {}      # script_name -> 活跃标志
        self.script_setting: dict = {}                      # script_name -> {script_path, parameters}

        # 当前线程所运行脚本的 name，供 is_active()/strategy_active 按线程解析
        self._current_script: threading.local = threading.local()

        self.datafeed: BaseDatafeed = get_datafeed()

        log_engine: LogEngine = self.main_engine.get_engine("log")
        log_engine.register_log(EVENT_SCRIPT_LOG)

    def init_engine(self) -> None:
        """启动脚本策略引擎：初始化数据服务并加载持久化记录（不自动启动）。"""
        result: bool = self.datafeed.init()
        if result:
            self.write_log("数据服务初始化成功")

        self.load_script_setting()
        self.write_log("脚本策略引擎初始化成功")

    # 向后兼容：cli 及旧代码调用的 init()
    init = init_engine

    def add_script(self, script_name: str, script_path: str, parameters: dict) -> None:
        """添加一条脚本记录。"""
        if script_name in self.scripts:
            self.write_log(f"创建脚本失败，存在重名 {script_name}")
            return

        script_data: ScriptData = ScriptData(
            script_name=script_name,
            script_path=script_path,
            parameters=parameters,
            inited=True,
            trading=False,
            class_name=Path(script_path).stem,
        )
        self.scripts[script_name] = script_data
        self.script_active_flags[script_name] = False

        self.update_script_setting(script_name, script_path, parameters)
        self.put_script_event(script_data)

    def remove_script(self, script_name: str) -> bool:
        """移除一条脚本记录，运行中则拒绝。"""
        script_data: ScriptData | None = self.scripts.get(script_name)
        if not script_data:
            return False

        if script_data.trading:
            self.write_log(f"脚本 {script_name} 移除失败，请先停止")
            return False

        self.remove_script_setting(script_name)
        self.scripts.pop(script_name)
        self.script_active_flags.pop(script_name, None)
        self.script_threads.pop(script_name, None)

        self.write_log(f"脚本 {script_name} 移除成功")
        return True

    def edit_script(self, script_name: str, parameters: dict) -> None:
        """编辑脚本参数，运行中则拒绝。"""
        script_data: ScriptData = self.scripts[script_name]
        if script_data.trading:
            self.write_log(f"脚本 {script_name} 编辑失败，请先停止")
            return

        script_data.parameters = parameters
        self.update_script_setting(script_name, script_data.script_path, parameters)
        self.put_script_event(script_data)

    def start_script(self, script_name: str) -> None:
        """在独立线程中启动一条脚本记录。"""
        script_data: ScriptData = self.scripts[script_name]
        if script_data.trading:
            self.write_log(f"{script_name} 已经启动，请勿重复操作")
            return

        self.script_active_flags[script_name] = True
        script_data.trading = True
        self.put_script_event(script_data)

        thread: Thread = Thread(target=self.run_script, args=(script_name,))
        self.script_threads[script_name] = thread
        thread.start()

        self.write_log(f"脚本 {script_name} 启动", script_name)

    def run_script(self, script_name: str) -> None:
        """加载脚本模块并调用 module.run(self, **parameters)。"""
        script_data: ScriptData = self.scripts[script_name]
        script_path: str = script_data.script_path
        parameters: dict = script_data.parameters

        path: Path = Path(script_path)
        if str(path.parent) not in sys.path:
            sys.path.append(str(path.parent))

        module_name: str = path.stem

        # 登记当前线程对应的脚本名，供 is_active()/strategy_active 解析
        self._current_script.name = script_name

        try:
            module: ModuleType = importlib.import_module(module_name)
            importlib.reload(module)

            # 反射 run() 签名组装 kwargs：排除 engine，优先用记录里的值，其次默认值
            sig: inspect.Signature = inspect.signature(module.run)
            kwargs: dict = {}
            for name, param in sig.parameters.items():
                if name == "engine":
                    continue
                if name in parameters:
                    kwargs[name] = parameters[name]
                elif param.default is not inspect.Parameter.empty:
                    kwargs[name] = param.default
                else:
                    self.write_log(f"参数 {name} 未提供且无默认值，跳过", script_name)

            module.run(self, **kwargs)
        except Exception:
            msg: str = f"触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg, script_name)
        finally:
            self.script_active_flags[script_name] = False
            script_data.trading = False
            self.script_threads.pop(script_name, None)
            self._current_script.name = None
            self.put_script_event(script_data)

    def stop_script(self, script_name: str) -> None:
        """置活跃标志为 False 通知脚本退出，并 join 线程。"""
        script_data: ScriptData | None = self.scripts.get(script_name)
        if not script_data or not script_data.trading:
            return

        self.script_active_flags[script_name] = False

        thread: Thread | None = self.script_threads.get(script_name)
        if thread:
            thread.join(timeout=5)
        self.script_threads.pop(script_name, None)

        script_data.trading = False
        self.put_script_event(script_data)
        self.write_log(f"脚本 {script_name} 停止", script_name)

    def start_all_scripts(self) -> None:
        """启动全部脚本记录。"""
        for script_name in list(self.scripts.keys()):
            self.start_script(script_name)

    def stop_all_scripts(self) -> None:
        """停止全部脚本记录。"""
        for script_name in list(self.scripts.keys()):
            self.stop_script(script_name)

    def close(self) -> None:
        """"""
        self.stop_all_scripts()

    # ---- 停止 API（脚本侧） ----

    def is_active(self) -> bool:
        """当前线程所运行脚本是否仍活跃。

        脚本在长循环里调用 ``while engine.is_active():`` 即可响应停止。
        """
        name: str | None = getattr(self._current_script, "name", None)
        if not name:
            return False
        return self.script_active_flags.get(name, False)

    @property
    def strategy_active(self) -> bool:
        """向后兼容：旧脚本 ``engine.strategy_active`` 读法等价于 is_active()。"""
        return self.is_active()

    def is_script_active(self, script_name: str) -> bool:
        """显式按名查询某条记录的活跃状态（UI/外部用）。"""
        return self.script_active_flags.get(script_name, False)

    # ---- 持久化 ----

    def load_script_setting(self) -> None:
        """加载配置文件并逐条注册记录（不自动启动）。"""
        self.script_setting = load_json(self.setting_filename)

        for script_name, config in self.script_setting.items():
            self.add_script(
                script_name,
                config["script_path"],
                config["parameters"]
            )

    def update_script_setting(
        self, script_name: str, script_path: str, parameters: dict
    ) -> None:
        """更新/追加一条记录到配置文件。"""
        self.script_setting[script_name] = {
            "script_path": script_path,
            "parameters": parameters,
        }
        save_json(self.setting_filename, self.script_setting)

    def remove_script_setting(self, script_name: str) -> None:
        """从配置文件移除一条记录。"""
        if script_name not in self.script_setting:
            return
        self.script_setting.pop(script_name)
        save_json(self.setting_filename, self.script_setting)

    # ---- 事件与反射 ----

    def put_script_event(self, script_data: ScriptData) -> None:
        """广播事件以更新 UI 卡片状态。"""
        data: dict = script_data.get_data()
        event: Event = Event(EVENT_SCRIPT_STRATEGY, data)
        self.event_engine.put(event)

    def get_script_parameters(self, script_path: str) -> dict:
        """导入脚本模块并反射 run()，返回排除 engine 后的参数默认值字典。"""
        path: Path = Path(script_path)
        if str(path.parent) not in sys.path:
            sys.path.append(str(path.parent))

        module_name: str = path.stem
        module: ModuleType = importlib.import_module(module_name)
        importlib.reload(module)

        parameters: dict = {}
        sig: inspect.Signature = inspect.signature(module.run)
        for name, param in sig.parameters.items():
            if name == "engine":
                continue
            if param.default is not inspect.Parameter.empty:
                parameters[name] = param.default
            else:
                parameters[name] = ""

        return parameters

    def connect_gateway(self, setting: dict, gateway_name: str) -> None:
        """"""
        self.main_engine.connect(setting, gateway_name)

    def send_order(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        direction: Direction,
        offset: Offset,
        order_type: OrderType
    ) -> str:
        """"""
        contract: ContractData | None = self.get_contract(vt_symbol)
        if not contract:
            return ""

        req: OrderRequest = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=order_type,
            volume=volume,
            price=price,
            offset=offset,
            reference=APP_NAME
        )

        vt_orderid: str = self.main_engine.send_order(req, contract.gateway_name)
        return vt_orderid

    def subscribe(self, vt_symbols: Sequence[str]) -> None:
        """"""
        for vt_symbol in vt_symbols:
            contract: ContractData | None = self.main_engine.get_contract(vt_symbol)
            if contract:
                req: SubscribeRequest = SubscribeRequest(
                    symbol=contract.symbol,
                    exchange=contract.exchange
                )
                self.main_engine.subscribe(req, contract.gateway_name)

    def buy(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT
    ) -> str:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.LONG, Offset.OPEN, order_type)

    def sell(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT
    ) -> str:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.SHORT, Offset.CLOSE, order_type)

    def short(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT
    ) -> str:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.SHORT, Offset.OPEN, order_type)

    def cover(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT
    ) -> str:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.LONG, Offset.CLOSE, order_type)

    def cancel_order(self, vt_orderid: str) -> None:
        """"""
        order: OrderData | None = self.get_order(vt_orderid)
        if not order:
            return

        req: CancelRequest = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def get_tick(self, vt_symbol: str, use_df: bool = False) -> TickData | None:
        """"""
        return get_data(self.main_engine.get_tick, arg=vt_symbol, use_df=use_df)

    def get_ticks(self, vt_symbols: Sequence[str], use_df: bool = False) -> Sequence[TickData] | DataFrame | None:
        """"""
        ticks: list = []
        for vt_symbol in vt_symbols:
            tick: TickData | None = self.main_engine.get_tick(vt_symbol)
            ticks.append(tick)

        if not use_df:
            return ticks
        else:
            return to_df(ticks)

    def get_order(self, vt_orderid: str, use_df: bool = False) -> OrderData | None:
        """"""
        return get_data(self.main_engine.get_order, arg=vt_orderid, use_df=use_df)

    def get_orders(self, vt_orderids: Sequence[str], use_df: bool = False) -> Sequence[OrderData] | DataFrame | None:
        """"""
        orders: list = []
        for vt_orderid in vt_orderids:
            order: OrderData | None = self.main_engine.get_order(vt_orderid)
            orders.append(order)

        if not use_df:
            return orders
        else:
            return to_df(orders)

    def get_trades(self, vt_orderid: str, use_df: bool = False) -> Sequence[TradeData] | DataFrame | None:
        """"""
        trades: list = []
        all_trades: list[TradeData] = self.main_engine.get_all_trades()

        for trade in all_trades:
            if trade.vt_orderid == vt_orderid:
                trades.append(trade)

        if not use_df:
            return trades
        else:
            return to_df(trades)

    def get_all_active_orders(self, use_df: bool = False) -> Sequence[OrderData] | DataFrame | None:
        """"""
        return get_data(self.main_engine.get_all_active_orders, use_df=use_df)

    def get_contract(self, vt_symbol: str, use_df: bool = False) -> ContractData | None:
        """"""
        return get_data(self.main_engine.get_contract, arg=vt_symbol, use_df=use_df)

    def get_all_contracts(self, use_df: bool = False) -> Sequence[ContractData] | DataFrame | None:
        """"""
        return get_data(self.main_engine.get_all_contracts, use_df=use_df)

    def get_account(self, vt_accountid: str, use_df: bool = False) -> AccountData | None:
        """"""
        return get_data(self.main_engine.get_account, arg=vt_accountid, use_df=use_df)

    def get_all_accounts(self, use_df: bool = False) -> Sequence[AccountData] | DataFrame | None:
        """"""
        return get_data(self.main_engine.get_all_accounts, use_df=use_df)

    def get_position(self, vt_positionid: str, use_df: bool = False) -> PositionData | None:
        """"""
        return get_data(self.main_engine.get_position, arg=vt_positionid, use_df=use_df)

    def get_position_by_symbol(self, vt_symbol: str, direction: Direction, use_df: bool = False) -> PositionData | None:
        """"""
        contract: ContractData = self.main_engine.get_contract(vt_symbol)
        if not contract:
            return None

        vt_positionid: str = f"{contract.gateway_name}.{contract.vt_symbol}.{direction.value}"
        return get_data(self.main_engine.get_position, arg=vt_positionid, use_df=use_df)

    def get_all_positions(self, use_df: bool = False) -> Sequence[PositionData] | DataFrame | None:
        """"""
        return get_data(self.main_engine.get_all_positions, use_df=use_df)

    def get_bars(
        self,
        vt_symbol: str,
        start_date: str,
        interval: Interval,
        use_df: bool = False
    ) -> Sequence[BarData]:
        """"""
        contract: ContractData | None = self.main_engine.get_contract(vt_symbol)
        if not contract:
            return []

        start: datetime = datetime.strptime(start_date, "%Y%m%d")
        end: datetime = datetime.now()

        req: HistoryRequest = HistoryRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            start=start,
            end=end,
            interval=interval
        )

        bars: Sequence[BarData] | DataFrame = get_data(self.datafeed.query_bar_history, arg=req, use_df=use_df)
        return bars

    def write_log(self, msg: str, script_name: str | None = None) -> None:
        """"""
        if script_name:
            msg = f"[{script_name}]  {msg}"

        log: LogData = LogData(msg=msg, gateway_name=APP_NAME)
        print(f"{log.time}\t{log.msg}")

        event: Event = Event(EVENT_SCRIPT_LOG, log)
        self.event_engine.put(event)

    def send_notification(self, msg: str) -> None:
        """"""
        subject: str = "脚本策略引擎通知"
        self.main_engine.send_notification(msg, subject)

    send_email = send_notification


def to_df(data_list: Sequence[BaseData]) -> DataFrame | None:
    """"""
    if not data_list:
        return None

    dict_list: list = [data.__dict__ for data in data_list if data]
    return DataFrame(dict_list)


def get_data(func: Callable, arg: Any = None, use_df: bool = False) -> Any:
    """"""
    if not arg:
        data = func()
    else:
        data = func(arg)

    if not use_df:
        return data
    elif data is None:
        return data
    else:
        if not isinstance(data, list):
            data = [data]
        return to_df(data)
