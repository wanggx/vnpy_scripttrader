"""同步 xtquant 板块及其成分到 SqlApp。

表 ``xtquant_sector`` 以 ``sector_name1/2/3`` 保存申万三级分类路径，表
``xtquant_sector_member`` 在同一路径上增加标的代码和名称。脚本先在内存中完整
构建快照，再用单个数据库事务替换旧数据；因此重复执行结果一致，抓取中断或异常
也不会留下半份快照。
"""

from __future__ import annotations

from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Any

from xtquant import xtdata
from vnpy_sqlapp import APP_NAME

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine
    from vnpy_sqlapp import SqlEngine


SECTOR_TABLE: str = "xtquant_sector"
MEMBER_TABLE: str = "xtquant_sector_member"

# 每次运行前更新板块文件，确保新板块及最新成分可见。
REFRESH_SECTOR_DATA: bool = True

# 使用申万一级、二级、三级行业；同一标的可在三个层级分别建立关系。
INCLUDED_SECTOR_PREFIXES: tuple[str, ...] = ("SW1", "SW2", "SW3")

# xtquant 最新板块缓存中的分类目录。与上面的前缀共同约束同步范围。
INCLUDED_SECTOR_CATEGORIES: tuple[str, ...] = ("申万行业",)

# 申万板块同时提供普通和“加权”版本，成分关系基本重复，只保留普通版本。
EXCLUDED_SECTOR_SUFFIXES: tuple[str, ...] = ("加权",)

# 只保存当前仍属于沪深京 A 股池的成分，退市及其他市场标的不会落库。
A_SHARE_SECTOR: str = "沪深京A股"

# 尚未进入申万分类的新股使用统一占位值，确保当前 A 股仍能保存到关系表。
UNCLASSIFIED_NAME: str = "未分类"

# 控制进度日志频率，不影响抓取或写库批次。
PROGRESS_INTERVAL: int = 100

# 数据库每批插入行数；每批完成后输出一次日志，所有批次仍处于同一事务。
INSERT_BATCH_SIZE: int = 10_000

# 批量读取合约名称时的进度粒度。
NAME_BATCH_SIZE: int = 500


def _is_candidate_sector(sector: str) -> bool:
    """判断板块是否属于默认同步范围。"""
    return sector.startswith(INCLUDED_SECTOR_PREFIXES) and not sector.endswith(
        EXCLUDED_SECTOR_SUFFIXES
    )


def _get_sector_level(sector: str) -> int | None:
    """从 SW1/SW2/SW3 板块名称解析申万行业层级。"""
    for level in (1, 2, 3):
        if sector.startswith(f"SW{level}"):
            return level
    return None


def _get_local_sector_sources(
    all_sectors: set[str],
) -> dict[str, list[Path]]:
    """返回 xtquant 最新板块缓存中的板块文件；目录不存在时返回空字典。"""
    try:
        data_dir: str = str(xtdata.get_client().get_data_dir())
    except (AttributeError, OSError):
        return {}
    template_dir: Path = Path(data_dir) / "Sector" / "Temple"
    if not template_dir.is_dir():
        return {}

    sources: dict[str, list[Path]] = {}
    for category in INCLUDED_SECTOR_CATEGORIES:
        category_dir: Path = template_dir / category
        if not category_dir.is_dir():
            continue
        for path in category_dir.iterdir():
            sector: str = path.name.strip()
            if not path.is_file() or sector not in all_sectors:
                continue
            if not _is_candidate_sector(sector):
                continue
            sources.setdefault(sector, []).append(path)
    return sources


def _read_local_sector_codes(paths: list[Path]) -> set[str]:
    """读取一个板块对应的本地最新成分文件。"""
    codes: set[str] = set()
    for path in paths:
        content: str = path.read_text(encoding="utf-8-sig")
        codes.update(code.strip() for code in content.split(",") if code.strip())
    return codes


def _get_stock_names(
    engine: ScriptEngine,
    stock_codes: list[str],
) -> dict[str, str] | None:
    """每个唯一标的只读取一次名称；用户停止时返回 ``None``。"""
    names: dict[str, str] = {}
    total: int = len(stock_codes)

    for start in range(0, total, NAME_BATCH_SIZE):
        if not engine.strategy_active:
            engine.write_log(
                f"同步已停止：已读取 {start}/{total} 个标的名称，数据库未修改"
            )
            return None

        batch: list[str] = stock_codes[start : start + NAME_BATCH_SIZE]
        details: dict[str, Any] = xtdata.get_instrument_detail_list(batch) or {}
        for code in batch:
            detail: dict[str, Any] | None = details.get(code)
            names[code] = str((detail or {}).get("InstrumentName", "")).strip()

        finished: int = min(start + len(batch), total)
        engine.write_log(f"标的名称读取进度：{finished}/{total}")

    missing: int = sum(not name for name in names.values())
    if missing:
        engine.write_log(f"有 {missing} 个标的未返回名称，将以空字符串保存")
    return names


def _collect_snapshot(
    engine: ScriptEngine,
) -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str, str, str]]] | None:
    """从 xtquant 构建完整去重快照；用户停止时返回 ``None``。"""
    if REFRESH_SECTOR_DATA:
        engine.write_log("正在更新 xtquant 板块数据")
        xtdata.download_sector_data()

    raw_sectors: list[str] = xtdata.get_sector_list() or []
    all_sectors: set[str] = {
        str(sector).strip() for sector in raw_sectors if str(sector).strip()
    }
    if not all_sectors:
        raise RuntimeError(
            "xtquant 未返回任何板块，请确认 MiniQMT 已启动且行情服务可用"
        )

    local_sources: dict[str, list[Path]] = _get_local_sector_sources(all_sectors)
    if local_sources:
        sectors: list[str] = sorted(local_sources)
        source_name: str = "xtquant 本地最新板块缓存"
    else:
        sectors = sorted(
            sector for sector in all_sectors if _is_candidate_sector(sector)
        )
        source_name = "xtquant 板块 API（本地分类目录不可用）"
    if not sectors:
        raise RuntimeError(
            "xtquant 板块列表中没有匹配配置范围的板块，请检查板块数据或前缀配置"
        )

    active_codes: set[str] = {
        str(code).strip()
        for code in (xtdata.get_stock_list_in_sector(A_SHARE_SECTOR) or [])
        if str(code).strip()
    }
    if not active_codes:
        raise RuntimeError(
            f"xtquant 未返回“{A_SHARE_SECTOR}”成分，请确认板块数据已更新"
        )

    engine.write_log(
        f"原始板块 {len(all_sectors)} 个，按分类筛选为 {len(sectors)} 个；"
        f"当前 A 股池 {len(active_codes)} 个；来源：{source_name}"
    )
    stock_sectors: dict[str, dict[int, set[str]]] = {
        code: {1: set(), 2: set(), 3: set()} for code in active_codes
    }
    valid_sector_count: int = 0
    raw_relation_count: int = 0

    for index, sector in enumerate(sectors, start=1):
        if not engine.strategy_active:
            engine.write_log(
                f"同步已停止：已读取 {index - 1}/{len(sectors)} 个板块，数据库未修改"
            )
            return None

        if local_sources:
            raw_codes: set[str] = _read_local_sector_codes(local_sources[sector])
        else:
            raw_codes = set(xtdata.get_stock_list_in_sector(sector) or [])
        codes: set[str] = {
            str(code).strip() for code in raw_codes if str(code).strip() in active_codes
        }
        level: int | None = _get_sector_level(sector)
        if codes and level is not None:
            valid_sector_count += 1
            raw_relation_count += len(codes)
            for code in codes:
                stock_sectors[code][level].add(sector)

        if index == 1 or index % PROGRESS_INTERVAL == 0 or index == len(sectors):
            engine.write_log(
                f"板块读取进度：{index}/{len(sectors)}，"
                f"有效板块 {valid_sector_count} 个、{raw_relation_count} 条原始关系"
            )

    if not valid_sector_count:
        raise RuntimeError("筛选后没有包含当前 A 股的有效板块，数据库未修改")

    relation_keys: list[tuple[str, str, str, str]] = []
    for code in sorted(active_codes):
        levels: dict[int, set[str]] = stock_sectors[code]
        names1: list[str] = sorted(levels[1]) or [UNCLASSIFIED_NAME]
        names2: list[str] = sorted(levels[2]) or [UNCLASSIFIED_NAME]
        names3: list[str] = sorted(levels[3]) or [UNCLASSIFIED_NAME]
        relation_keys.extend(
            (name1, name2, name3, code)
            for name1, name2, name3 in product(names1, names2, names3)
        )

    sector_paths: list[tuple[str, str, str]] = sorted(
        {(name1, name2, name3) for name1, name2, name3, _ in relation_keys}
    )
    engine.write_log(
        f"整理出 {len(sector_paths)} 条申万三级分类路径、"
        f"{len(relation_keys)} 条标的关系"
    )

    stock_codes: list[str] = sorted(active_codes)
    engine.write_log(f"开始读取 {len(stock_codes)} 个唯一标的名称")
    stock_names: dict[str, str] | None = _get_stock_names(engine, stock_codes)
    if stock_names is None:
        return None

    named_members: list[tuple[str, str, str, str, str]] = [
        (name1, name2, name3, code, stock_names.get(code, ""))
        for name1, name2, name3, code in relation_keys
    ]
    return sector_paths, named_members


def _ensure_tables(sql_engine: SqlEngine) -> None:
    """不存在时创建两张申万三级分类快照表。"""
    sql_engine.execute(
        f"CREATE TABLE IF NOT EXISTS {SECTOR_TABLE} ("
        "sector_name1 VARCHAR(128) NOT NULL, "
        "sector_name2 VARCHAR(128) NOT NULL, "
        "sector_name3 VARCHAR(128) NOT NULL, "
        "PRIMARY KEY (sector_name1, sector_name2, sector_name3)"
        ")"
    )
    sql_engine.execute(
        f"CREATE TABLE IF NOT EXISTS {MEMBER_TABLE} ("
        "sector_name1 VARCHAR(128) NOT NULL, "
        "sector_name2 VARCHAR(128) NOT NULL, "
        "sector_name3 VARCHAR(128) NOT NULL, "
        "stock_code VARCHAR(64) NOT NULL, "
        "stock_name VARCHAR(128) NOT NULL DEFAULT '', "
        "PRIMARY KEY (sector_name1, sector_name2, sector_name3, stock_code), "
        "FOREIGN KEY (sector_name1, sector_name2, sector_name3) "
        f"REFERENCES {SECTOR_TABLE}(sector_name1, sector_name2, sector_name3)"
        ")"
    )


def _replace_snapshot(
    sql_engine: SqlEngine,
    driver: str,
    sectors: list[tuple[str, str, str]],
    members: list[tuple[str, str, str, str, str]],
    engine: ScriptEngine,
) -> None:
    """在一个事务中精确替换快照，保证幂等性和失败回滚。"""
    placeholder: str = "?" if driver == "sqlite" else "%s"
    insert_sector: str = (
        f"INSERT INTO {SECTOR_TABLE} (sector_name1, sector_name2, sector_name3) "
        f"VALUES ({placeholder}, {placeholder}, {placeholder})"
    )
    insert_member: str = (
        f"INSERT INTO {MEMBER_TABLE} "
        f"(sector_name1, sector_name2, sector_name3, stock_code, stock_name) "
        f"VALUES ({placeholder}, {placeholder}, {placeholder}, "
        f"{placeholder}, {placeholder})"
    )

    with sql_engine.transaction() as conn:
        # 先删子表以满足外键约束，再按父表、子表顺序写入完整快照。
        conn.execute(f"DELETE FROM {MEMBER_TABLE}")
        conn.execute(f"DELETE FROM {SECTOR_TABLE}")

        sector_rows: list[tuple[Any, ...]] = [tuple(row) for row in sectors]
        member_rows: list[tuple[Any, ...]] = [tuple(row) for row in members]
        write_groups: tuple[tuple[str, str, list[tuple[Any, ...]]], ...] = (
            ("板块", insert_sector, sector_rows),
            ("板块成分", insert_member, member_rows),
        )
        for label, sql, rows in write_groups:
            total: int = len(rows)
            batch_count: int = (total + INSERT_BATCH_SIZE - 1) // INSERT_BATCH_SIZE
            for start in range(0, total, INSERT_BATCH_SIZE):
                batch: list[tuple[Any, ...]] = rows[start : start + INSERT_BATCH_SIZE]
                conn.executemany(sql, batch)
                finished: int = min(start + len(batch), total)
                batch_number: int = start // INSERT_BATCH_SIZE + 1
                engine.write_log(
                    f"{driver.upper()} 写入进度：{label} {finished}/{total}，"
                    f"第 {batch_number}/{batch_count} 批完成"
                )


def run(engine: ScriptEngine) -> None:
    """ScriptTrader 策略入口：读取 xtquant 板块快照并写入 SqlApp。"""
    sql_engine: SqlEngine | None = engine.main_engine.get_engine(APP_NAME)
    if sql_engine is None:
        raise RuntimeError(
            "板块同步脚本依赖 SqlApp，请先加载 SqlApp（script/run.py 中 add_app(SqlApp)）"
        )

    driver: str = getattr(sql_engine.database, "driver_name", "sqlite")
    engine.write_log(f"SqlApp 已就绪，数据库驱动：{driver}")

    snapshot: (
        tuple[
            list[tuple[str, str, str]],
            list[tuple[str, str, str, str, str]],
        ]
        | None
    ) = _collect_snapshot(engine)
    if snapshot is None:
        return
    sectors, members = snapshot

    if not engine.strategy_active:
        engine.write_log("同步已停止，数据库未修改")
        return

    _ensure_tables(sql_engine)
    _replace_snapshot(sql_engine, driver, sectors, members, engine)
    engine.write_log(f"板块同步完成：{len(sectors)} 个板块、{len(members)} 条成分关系")
