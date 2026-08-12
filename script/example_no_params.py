"""示例：无可配置参数的脚本。

run(engine) 只接收 engine，反射后无可编辑参数。
在「添加脚本」对话框中选定后，参数区会提示「该脚本 run() 无可配置参数」，
直接点「添加」即可用空参数启动。
"""
from datetime import datetime

from vnpy_scripttrader.engine import ScriptEngine


def run(engine: ScriptEngine) -> None:
    """每 5 秒打印一次心跳，直到停止。"""
    engine.write_log("无参数脚本启动")

    while engine.is_active():
        ts = datetime.now().strftime("%H:%M:%S")
        engine.write_log(f"心跳 {ts}")

        for _ in range(50):
            if not engine.is_active():
                break
            import time
            time.sleep(0.1)

    engine.write_log("无参数脚本已停止")
