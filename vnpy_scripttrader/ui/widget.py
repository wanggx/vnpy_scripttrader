import traceback
from pathlib import Path

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtWidgets, QtCore, QtGui
from vnpy.trader.object import LogData

from ..base import APP_NAME, EVENT_SCRIPT_LOG, EVENT_SCRIPT_STRATEGY
from ..engine import ScriptEngine


class ScriptManager(QtWidgets.QWidget):
    """"""
    signal_log: QtCore.Signal = QtCore.Signal(Event)
    signal_script: QtCore.Signal = QtCore.Signal(Event)

    # 脚本表列定义
    COL_NAME: int = 0
    COL_PATH: int = 1
    COL_PARAM: int = 2
    COL_STATUS: int = 3
    COL_OPS: int = 4

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.script_engine: ScriptEngine = main_engine.get_engine(APP_NAME)

        # script_name -> 行号
        self.row_map: dict[str, int] = {}

        self.init_ui()
        self.register_event()

        self.script_engine.init_engine()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("脚本策略")

        # 左面板顶栏：添加脚本 … 全部启动 / 全部停止（与左侧表格右边缘对齐）
        add_button: QtWidgets.QPushButton = QtWidgets.QPushButton("添加脚本")
        add_button.clicked.connect(self.add_script)

        start_all_button: QtWidgets.QPushButton = QtWidgets.QPushButton("全部启动")
        start_all_button.clicked.connect(self.script_engine.start_all_scripts)

        stop_all_button: QtWidgets.QPushButton = QtWidgets.QPushButton("全部停止")
        stop_all_button.clicked.connect(self.script_engine.stop_all_scripts)

        left_toolbar: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        left_toolbar.addWidget(add_button)
        left_toolbar.addStretch()
        left_toolbar.addWidget(start_all_button)
        left_toolbar.addWidget(stop_all_button)

        # 脚本记录表（替代原卡片滚动区，提供表格样式）
        self.script_table: QtWidgets.QTableWidget = QtWidgets.QTableWidget()
        self.script_table.setColumnCount(5)
        self.script_table.setHorizontalHeaderLabels(["名称", "路径", "参数", "状态", "操作"])
        self.script_table.verticalHeader().setVisible(False)
        self.script_table.setEditTriggers(
            self.script_table.EditTrigger.NoEditTriggers
        )
        self.script_table.setAlternatingRowColors(True)
        self.script_table.setSelectionBehavior(
            self.script_table.SelectionBehavior.SelectRows
        )

        header: QtWidgets.QHeaderView = self.script_table.horizontalHeader()
        # 全部列用 Interactive：用户可手动拖动列宽；最后一列 Stretch 占满剩余宽度
        header.setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)

        # 初始列宽
        self.script_table.setColumnWidth(self.COL_NAME, 120)
        self.script_table.setColumnWidth(self.COL_PATH, 280)
        self.script_table.setColumnWidth(self.COL_PARAM, 200)
        self.script_table.setColumnWidth(self.COL_STATUS, 80)
        self.script_table.setColumnWidth(self.COL_OPS, 240)

        left_panel: QtWidgets.QWidget = QtWidgets.QWidget()
        left_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout(left_panel)
        left_vbox.addLayout(left_toolbar)
        left_vbox.addWidget(self.script_table)

        # 右面板顶栏：清空日志（贴右侧）
        clear_button: QtWidgets.QPushButton = QtWidgets.QPushButton("清空日志")
        clear_button.clicked.connect(self.clear_log)

        right_toolbar: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        right_toolbar.addStretch()
        right_toolbar.addWidget(clear_button)

        # 日志
        self.log_monitor: QtWidgets.QTextEdit = QtWidgets.QTextEdit()
        self.log_monitor.setReadOnly(True)

        right_panel: QtWidgets.QWidget = QtWidgets.QWidget()
        right_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout(right_panel)
        right_vbox.addLayout(right_toolbar)
        right_vbox.addWidget(self.log_monitor)

        # 左右可拖动分隔条：顶栏随各面板一起移动
        splitter: QtWidgets.QSplitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(splitter)

        self.setLayout(vbox)

    def register_event(self) -> None:
        """"""
        self.signal_log.connect(self.process_log_event)
        self.event_engine.register(EVENT_SCRIPT_LOG, self.signal_log.emit)

        self.signal_script.connect(self.process_script_event)
        self.event_engine.register(EVENT_SCRIPT_STRATEGY, self.signal_script.emit)

    def show(self) -> None:
        """"""
        self.showMaximized()

    def process_log_event(self, event: Event) -> None:
        """"""
        log: LogData = event.data
        msg: str = f"{log.time}\t{log.msg}"
        self.log_monitor.append(msg)

    def process_script_event(self, event: Event) -> None:
        """根据事件更新或新增表格行。"""
        data: dict = event.data
        script_name: str = data["script_name"]

        if script_name in self.row_map:
            self.update_row(script_name, data)
        else:
            self.insert_row(script_name, data)

    def insert_row(self, script_name: str, data: dict) -> None:
        """新增一行脚本记录。"""
        row: int = self.script_table.rowCount()
        self.script_table.insertRow(row)
        self.row_map[script_name] = row

        self.script_table.setItem(row, self.COL_NAME, QtWidgets.QTableWidgetItem(script_name))
        self.script_table.setItem(row, self.COL_PATH, QtWidgets.QTableWidgetItem(data["script_path"]))

        param_item: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem()
        self.script_table.setItem(row, self.COL_PARAM, param_item)

        status_item: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem()
        self.script_table.setItem(row, self.COL_STATUS, status_item)

        ops: ScriptOpsWidget = ScriptOpsWidget(self, self.script_engine, script_name)
        self.script_table.setCellWidget(row, self.COL_OPS, ops)

        self.update_row(script_name, data)

    def update_row(self, script_name: str, data: dict) -> None:
        """更新某行的参数与状态。"""
        row: int = self.row_map[script_name]

        params: dict = data.get("parameters", {})
        param_str: str = ", ".join(f"{k}={v}" for k, v in params.items())
        self.script_table.item(row, self.COL_PARAM).setText(param_str if param_str else "(无参数)")

        status_item: QtWidgets.QTableWidgetItem = self.script_table.item(row, self.COL_STATUS)
        if data["trading"]:
            status_item.setText("运行中")
            status_item.setForeground(QtGui.QColor("#27ae60"))
        else:
            status_item.setText("已停止")
            status_item.setForeground(QtGui.QColor("#7f8c8d"))

        ops: ScriptOpsWidget = self.script_table.cellWidget(row, self.COL_OPS)
        if ops:
            ops.update_state(data["trading"])

    def remove_script(self, script_name: str) -> None:
        """从表格移除一行并重排行号。"""
        row: int = self.row_map.pop(script_name, -1)
        if row < 0:
            return

        self.script_table.removeRow(row)

        # 移除行后，其后所有行号前移一位
        for name, r in self.row_map.items():
            if r > row:
                self.row_map[name] = r - 1

    def add_script(self) -> None:
        """添加脚本：打开编辑器，在对话框内选择路径、填名称与参数后提交。"""
        editor: ScriptSettingEditor = ScriptSettingEditor(
            self.script_engine, {}, script_path="", script_name=""
        )
        n: int = editor.exec_()

        if n == editor.DialogCode.Accepted:
            setting: dict = editor.get_setting()
            script_name: str = setting.pop("script_name", "").strip()
            script_path: str = setting.pop("script_path", "").strip()

            if not script_name or not script_path:
                QtWidgets.QMessageBox.warning(
                    self, "信息不完整", "请填写脚本名称并选择脚本路径"
                )
                return

            self.script_engine.add_script(script_name, script_path, setting)

    def clear_log(self) -> None:
        """"""
        self.log_monitor.clear()


class ScriptOpsWidget(QtWidgets.QWidget):
    """表格「操作」单元格内的按钮组。"""

    def __init__(
        self,
        script_manager: ScriptManager,
        script_engine: ScriptEngine,
        script_name: str
    ) -> None:
        """"""
        super().__init__()

        self.script_manager: ScriptManager = script_manager
        self.script_engine: ScriptEngine = script_engine
        self.script_name: str = script_name

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.start_button: QtWidgets.QPushButton = QtWidgets.QPushButton("启动")
        self.start_button.clicked.connect(self.start_script)

        self.stop_button: QtWidgets.QPushButton = QtWidgets.QPushButton("停止")
        self.stop_button.clicked.connect(self.stop_script)

        self.edit_button: QtWidgets.QPushButton = QtWidgets.QPushButton("编辑")
        self.edit_button.clicked.connect(self.edit_script)

        self.remove_button: QtWidgets.QPushButton = QtWidgets.QPushButton("移除")
        self.remove_button.clicked.connect(self.remove_script)

        hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox.setContentsMargins(0, 0, 0, 0)
        hbox.addWidget(self.start_button)
        hbox.addWidget(self.stop_button)
        hbox.addWidget(self.edit_button)
        hbox.addWidget(self.remove_button)
        self.setLayout(hbox)

    def update_state(self, trading: bool) -> None:
        """"""
        self.start_button.setEnabled(not trading)
        self.stop_button.setEnabled(trading)
        self.edit_button.setEnabled(not trading)
        self.remove_button.setEnabled(not trading)

    def start_script(self) -> None:
        """"""
        self.script_engine.start_script(self.script_name)

    def stop_script(self) -> None:
        """"""
        self.script_engine.stop_script(self.script_name)

    def edit_script(self) -> None:
        """"""
        script_data = self.script_engine.scripts[self.script_name]

        # 反射取参数默认值，再用当前值覆盖，作为编辑器预填
        parameters: dict = self.script_engine.get_script_parameters(script_data.script_path)
        parameters.update(script_data.parameters)

        editor: ScriptSettingEditor = ScriptSettingEditor(
            self.script_engine,
            parameters,
            script_path=script_data.script_path,
            script_name=self.script_name
        )
        n: int = editor.exec_()

        if n == editor.DialogCode.Accepted:
            setting: dict = editor.get_setting()
            setting.pop("script_name", None)
            setting.pop("script_path", None)
            self.script_engine.edit_script(self.script_name, setting)

    def remove_script(self) -> None:
        """"""
        result: bool = self.script_engine.remove_script(self.script_name)
        if result:
            self.script_manager.remove_script(self.script_name)


class ScriptSettingEditor(QtWidgets.QDialog):
    """反射式参数编辑器：新增模式内含路径选择、名称与参数编辑。"""

    def __init__(
        self,
        script_engine: ScriptEngine,
        parameters: dict,
        script_path: str = "",
        script_name: str = ""
    ) -> None:
        """"""
        super().__init__()

        self.script_engine: ScriptEngine = script_engine
        self.parameters: dict = parameters
        self.script_path: str = script_path
        self.script_name: str = script_name

        self.is_new: bool = not bool(script_name)

        # 是否已选定脚本（用于区分“未选择”与“选择后无参数”两种空态）
        self.script_selected: bool = bool(script_path)

        # name -> (QLineEdit, type)
        self.edits: dict = {}

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("添加脚本" if self.is_new else f"参数编辑: {self.script_name}")
        button_text: str = "添加" if self.is_new else "确定"

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()

        # 脚本名称
        self.name_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit(self.script_name)
        form.addRow("脚本名称 (str)", self.name_edit)

        if self.is_new:
            # 脚本路径 + 选择按钮（在对话框内选路径）
            self.path_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit(self.script_path)
            self.path_edit.setReadOnly(True)

            browse_button: QtWidgets.QPushButton = QtWidgets.QPushButton("选择脚本")
            browse_button.clicked.connect(self.browse_script)

            path_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
            path_hbox.setContentsMargins(0, 0, 0, 0)
            path_hbox.addWidget(self.path_edit)
            path_hbox.addWidget(browse_button)

            path_widget: QtWidgets.QWidget = QtWidgets.QWidget()
            path_widget.setLayout(path_hbox)
            form.addRow("脚本路径 (str)", path_widget)

        # 参数区（独立分组框，给出明确的参数填写位置；选脚本后自动填充）
        param_group: QtWidgets.QGroupBox = QtWidgets.QGroupBox("脚本参数")
        self.param_form: QtWidgets.QFormLayout = QtWidgets.QFormLayout(param_group)
        self.param_form.setContentsMargins(8, 12, 8, 8)

        self.param_hint: QtWidgets.QLabel = QtWidgets.QLabel("（请先点击「选择脚本」，参数将自动列出）")
        self.param_hint.setStyleSheet("color:#7f8c8d;")
        self.no_param_hint: QtWidgets.QLabel = QtWidgets.QLabel("（该脚本 run() 无可配置参数）")
        self.no_param_hint.setStyleSheet("color:#27ae60;")

        form.addRow(param_group)
        self.refresh_params(self.parameters)

        # 提交按钮
        button: QtWidgets.QPushButton = QtWidgets.QPushButton(button_text)
        button.clicked.connect(self.accept)
        form.addRow(button)

        widget: QtWidgets.QWidget = QtWidgets.QWidget()
        widget.setLayout(form)

        scroll: QtWidgets.QScrollArea = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(widget)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(scroll)
        self.setLayout(vbox)

    def refresh_params(self, parameters: dict) -> None:
        """清空并按 parameters 重建参数输入行；为空时按选择状态给出提示。"""
        # 清空旧行
        while self.param_form.rowCount():
            self.param_form.takeRow(0)
        self.edits = {}

        if not parameters:
            # 区分“未选择”与“选择后无参数”
            self.param_form.addRow(
                self.no_param_hint if self.script_selected else self.param_hint
            )
            return

        for name, value in parameters.items():
            type_: type = type(value) if value != "" else str

            edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit(str(value))
            if type_ is int:
                edit.setValidator(QtGui.QIntValidator())
            elif type_ is float:
                edit.setValidator(QtGui.QDoubleValidator())

            self.param_form.addRow(f"{name} ({type_.__name__})", edit)
            self.edits[name] = (edit, type_)

    def browse_script(self) -> None:
        """在对话框内选择脚本文件，反射 run() 填充参数。"""
        cwd: str = str(Path.cwd())
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "载入策略脚本", cwd, "Python File(*.py)"
        )
        if not path:
            return

        try:
            parameters: dict = self.script_engine.get_script_parameters(path)
        except Exception:
            QtWidgets.QMessageBox.warning(
                self, "加载失败",
                f"无法加载脚本或反射 run() 失败:\n{traceback.format_exc()}"
            )
            return

        self.script_path = path
        self.script_selected = True
        self.path_edit.setText(path)

        # 名称未填时默认取文件名 stem
        if not self.name_edit.text().strip():
            self.name_edit.setText(Path(path).stem)

        self.refresh_params(parameters)

    def get_setting(self) -> dict:
        """"""
        setting: dict = {}

        setting["script_name"] = self.name_edit.text().strip()
        setting["script_path"] = self.script_path

        for name, tp in self.edits.items():
            edit, type_ = tp
            value_text: str = edit.text()

            if type_ is bool:
                value = value_text == "True"
            elif type_ is int:
                value = int(value_text) if value_text else 0
            elif type_ is float:
                value = float(value_text) if value_text else 0.0
            else:
                value = value_text

            setting[name] = value

        return setting
