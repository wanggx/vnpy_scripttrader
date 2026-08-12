"""示例：带可配置参数的脚本。

run() 签名中除 engine 之外的参数（带默认值）会被 ScriptEngine 反射为
可编辑参数，在「添加脚本」对话框中自动列出，支持 str / int / float / bool 类型。
"""
from datetime import datetime

from vnpy_scripttrader.engine import ScriptEngine


def run(
    engine: ScriptEngine,
    vt_symbol: str = "rb2501.SHFE",
    price: float = 3500.0,
    volume: int = 1,
    dry_run: bool = True,
    interval_seconds: int = 5,
) -> None:
    """每隔 interval_seconds 秒查一次行情并打印，dry_run 为 True 时不下单。

    参数说明：
      vt_symbol         行情合约代码
      price             触发价格（示例用，dry_run 下仅打印）
      volume            委托数量
      dry_run           只读模式，True=不下单
      interval_seconds  轮询间隔（秒）
    """
    engine.write_log(
        f"参数脚本启动：{vt_symbol} price={price} volume={volume} "
        f"dry_run={dry_run} interval={interval_seconds}s"
    )

    engine.subscribe([vt_symbol])

    while engine.is_active():
        tick = engine.get_tick(vt_symbol)
        if tick:
            ts = datetime.now().strftime("%H:%M:%S")
            engine.write_log(f"[{ts}] {vt_symbol} 最新价={tick.last_price}")

        if not dry_run:
            # 仅作示例：实际下单需结合自身策略逻辑
            engine.write_log(f"示例下单意图：买开 {volume} @ {price}")

        # 用 is_active 配合循环，停止时可在 interval_seconds 内退出
        engine.is_active()  # 占位，避免 hot-loop；下方 sleep 不阻塞停止响应
        for _ in range(interval_seconds * 10):
            if not engine.is_active():
                break
            import time
            time.sleep(0.1)

    engine.write_log("参数脚本已停止")
