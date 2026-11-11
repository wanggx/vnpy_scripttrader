# 1.2.0版本

1. 替换send_email为新的send_notification消息发送函数
2. **选股结果表的数值列改为定点小数**（原 `REAL`/`FLOAT`）：
   `stock_near_ma` 的 `close`/`high52w` → `DECIMAL(18,2)`；
   `stock_halt_downturn` 的 `close_price`/`prior_peak_price`/`vol_*` → `DECIMAL(18,2)`，
   `drop_from_peak_ratio`/`vol_shrink_ratio`/`doji_body_ratio`/`macd_*` → `DECIMAL(18,4)`，
   `kdj_*`/`rsi` → `DECIMAL(18,2)`。浮点既存前复权价的真精度（`12.533842189676422`），
   也存二进制尾巴（`11.540000000000001`）；定点后写入即按定义小数位四舍五入。
   已有表由新增的 `script/market/db_schema.py::ensure_decimal_columns()` 在建表后幂等迁移
   （`information_schema` 比对 + `ALTER COLUMN ... TYPE`；mysql 用 `MODIFY COLUMN`；
   sqlite 类型动态且无 `ALTER COLUMN TYPE`，跳过）。

# 1.1.1版本

1. 注册模块日志输出到日志引擎

# 1.1.0版本

1. vnpy框架4.0版本升级适配

# 1.0.3版本

1. 替换所有的Qt信号pyqtSignal为Signal

# 1.0.2版本

1. 增加基于vt_symbol和direction的持仓查询函数

# 1.0.1版本

1. 将模块的图标文件信息，改为完整路径字符串
