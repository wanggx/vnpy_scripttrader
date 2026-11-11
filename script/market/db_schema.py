"""结果表的数值列类型：定点小数（DECIMAL）与幂等迁移。

脚本自己建表（``stock_near_ma`` / ``stock_halt_downturn``），价格、量额与指标原先
用 ``REAL`` / ``FLOAT``（浮点），存进去后会留下长小数——既有真精度（前复权价
``12.533842189676422``），也有二进制尾巴（``11.540000000000001``）。改成定点小数
后写入即按定义的小数位四舍五入，SQL 查询与导出干净。

已有表不会因为 ``CREATE TABLE IF NOT EXISTS`` 改列类型，所以建表后调用
``ensure_decimal_columns`` 按 information_schema 比对，只对不一致的列执行一次
``ALTER``。幂等：类型已对时不做任何操作。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vnpy_sqlapp import SqlEngine


# 使用 ``%s`` 占位符的驱动；其余（sqlite/duckdb）用 ``?``
PERCENT_PLACEHOLDER_DRIVERS: frozenset[str] = frozenset({"mysql", "postgresql"})


def placeholder(driver: str) -> str:
    """参数占位符：mysql/postgresql 是 ``%s``，sqlite/duckdb 是 ``?``"""
    return "%s" if driver in PERCENT_PLACEHOLDER_DRIVERS else "?"


def ensure_decimal_columns(
    sql_engine: "SqlEngine",
    driver: str,
    table: str,
    spec: dict[str, str],
) -> list[str]:
    """把 ``spec`` 里的列改成指定定点类型，返回实际改动的 ``表.列`` 列表。

    ``spec`` 形如 ``{"close_price": "DECIMAL(18,2)", "macd_dif": "DECIMAL(18,4)"}``。
    - duckdb / postgresql：``ALTER TABLE t ALTER COLUMN c TYPE <type>``
    - mysql：``ALTER TABLE t MODIFY COLUMN c <type> NULL``（保留可空，避免误加 NOT NULL）
    - sqlite：跳过——SQLite 没有 ``ALTER COLUMN TYPE``，且其类型是动态的，本来就是
      按写入值原样存，改 schema 无收益。
    """
    if driver == "sqlite" or not spec:
        return []

    ph: str = placeholder(driver)
    result = sql_engine.query(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name = {ph}",
        (table,),
    )
    if result.error:
        return []

    current: dict[str, str] = {
        str(row[0]): str(row[1]).upper() for row in result.rows if row
    }

    altered: list[str] = []
    for column, target in spec.items():
        now: str | None = current.get(column)
        if now is None or now == target.upper():
            continue
        if driver == "mysql":
            sql: str = f"ALTER TABLE {table} MODIFY COLUMN {column} {target} NULL"
        else:
            sql = f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {target}"
        sql_engine.execute(sql)
        altered.append(f"{table}.{column}")
    return altered
