"""
Defines constants and objects used in ScriptTrader App.
"""

from dataclasses import dataclass, field
from datetime import datetime


APP_NAME = "ScriptTrader"

EVENT_SCRIPT_LOG = "eScriptLog"
EVENT_SCRIPT_STRATEGY = "eScriptStrategy"


@dataclass
class ScriptData:
    """脚本记录数据，通过 EVENT_SCRIPT_STRATEGY 广播给 UI。

    对齐 CtaTemplate.get_data() 模式：引擎组装该字典，UI 据此创建/更新卡片。
    脚本是带 ``run()`` 的普通模块而非类策略，故这里用轻量 dataclass。
    """

    script_name: str            # 记录名（唯一键）
    script_path: str            # .py 路径
    parameters: dict            # 传给 module.run 的 kwargs
    inited: bool = True         # 恒 True，脚本无独立初始化阶段
    trading: bool = False       # 运行中
    class_name: str = ""        # 文件名 stem，展示用
    datetime: datetime = field(default_factory=datetime.now)

    def get_data(self) -> dict:
        """返回可序列化字典供事件广播。"""
        return {
            "script_name": self.script_name,
            "script_path": self.script_path,
            "parameters": self.parameters,
            "inited": self.inited,
            "trading": self.trading,
            "class_name": self.class_name,
        }
