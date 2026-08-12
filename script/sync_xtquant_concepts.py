"""同步 xtquant 概念板块及其成分到 SqlApp。

与 ``sync_xtquant_sectors.py``（申万行业三级分类）互补：概念板块是扁平标签，
没有 1/2/3 层级，故单独建表 ``xtquant_concept_member``，按
``concept_category / concept_name / stock_code / stock_name`` 保存。

分类归属只能从 xtquant 本地 ``Sector/Temple/<分类>/`` 目录判断（API
``get_sector_list`` 返回扁平板块名，给不出分类）。本脚本自动发现 Temple 下所有
分类目录，排除 ``EXCLUDED_CATEGORIES``（申万行业已由另一脚本同步）后作为概念
来源；首轮日志会列出每个分类及其板块数，便于核对实际有多少个概念。

先在内存构建完整快照，再用单个事务替换旧数据，重复执行结果一致，抓取中断或
异常也不会留下半份快照。
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from xtquant import xtdata
from vnpy_sqlapp import APP_NAME

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine
    from vnpy_sqlapp import SqlEngine


CONCEPT_TABLE: str = "xtquant_concept_member"

# 每次运行前更新板块文件，确保新概念及最新成分可见。
REFRESH_SECTOR_DATA: bool = True

# 概念分类目录白名单。留空则自动取 Temple 下除 EXCLUDED_CATEGORIES 外的所有分类。
# 首轮日志会打印各分类及其板块数；确认后可在此固定，避免误纳指数/地区等分类。
CONCEPT_CATEGORIES: tuple[str, ...] = ()

# 非概念分类：申万行业已由 sync_xtquant_sectors.py 同步，默认排除。
# 指数板块/地区板块/风格板块等若不需要，运行后看日志再追加到此。
EXCLUDED_CATEGORIES: tuple[str, ...] = ("申万行业",)

# 板块名以这些后缀结尾的跳过（如申万"加权"版本，概念一般没有，防御性保留）。
EXCLUDED_SECTOR_SUFFIXES: tuple[str, ...] = ("加权",)

# 概念当前 A 股成分数少于此值则视为过期/残留概念，不入库。xtquant Temple 目录
# 是累积型的，download_sector_data 只增不删，历史上出现过的概念文件会永久残留；
# 这类过期概念的成分代码大半已退市，经 A 股池过滤后通常只剩寥寥几个。设此阈值
# 可砍掉历史长尾。设为 1 则不过滤（仅剔除成分为空的概念）。首轮先看直方图再调。
MIN_MEMBER_COUNT: int = 5

# 只保存当前仍属于沪深京 A 股池的成分，退市及其他市场标的不会落库。
A_SHARE_SECTOR: str = "沪深京A股"

# 控制进度日志频率，不影响抓取或写库批次。
PROGRESS_INTERVAL: int = 100

# 数据库每批插入行数；每批完成后输出一次日志，所有批次仍处于同一事务。
INSERT_BATCH_SIZE: int = 10_000

# 批量读取合约名称时的进度粒度。
NAME_BATCH_SIZE: int = 500


def _get_data_dir() -> Path | None:
    """返回 xtquant 数据目录，取不到时返回 None。"""
    try:
        return Path(str(xtdata.get_client().get_data_dir()))
    except (AttributeError, OSError):
        return None


def _read_local_sector_codes(paths: list[Path]) -> set[str]:
    """读取一个板块对应的本地最新成分文件。"""
    codes: set[str] = set()
    for path in paths:
        content: str = path.read_text(encoding="utf-8-sig")
        codes.update(code.strip() for code in content.split(",") if code.strip())
    return codes


def _is_candidate_sector(sector: str) -> bool:
    """板块名是否符合同步条件（仅排除后缀，分类由目录决定）。"""
    return not sector.endswith(EXCLUDED_SECTOR_SUFFIXES)


def _resolve_concept_categories(template_dir: Path) -> list[str]:
    """确定要同步的概念分类目录列表。

    ``CONCEPT_CATEGORIES`` 非空时直接用它；为空时取 Temple 下除
    ``EXCLUDED_CATEGORIES`` 外的所有子目录。
    """
    if CONCEPT_CATEGORIES:
        return [c for c in CONCEPT_CATEGORIES if (template_dir / c).is_dir()]

    discovered: list[str] = []
    for path in template_dir.iterdir():
        if not path.is_dir():
            continue
        if path.name in EXCLUDED_CATEGORIES:
            continue
        discovered.append(path.name)
    return sorted(discovered)


def _get_local_concept_sources(
    all_sectors: set[str],
    template_dir: Path,
    categories: list[str],
    engine: ScriptEngine,
) -> dict[str, dict[str, list[Path]]]:
    """返回 ``{category: {sector: [paths]}}``，仅含在 all_sectors 中的候选板块。

    同时按分类统计板块数并写日志，便于首轮核对实际概念数量。
    """
    sources: dict[str, dict[str, list[Path]]] = {}
    for category in categories:
        category_dir: Path = template_dir / category
        if not category_dir.is_dir():
            continue
        cat_sectors: dict[str, list[Path]] = {}
        for path in category_dir.iterdir():
            sector: str = path.name.strip()
            if not path.is_file() or sector not in all_sectors:
                continue
            if not _is_candidate_sector(sector):
                continue
            cat_sectors.setdefault(sector, []).append(path)
        sources[category] = cat_sectors
        engine.write_log(f"分类“{category}”：{len(cat_sectors)} 个板块")
    return sources


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


# 直方图分桶边界（含左不含右）：成分数落在 [lo, hi) 的概念归入此桶。
_HISTOGRAM_BUCKETS: tuple[tuple[int, int], ...] = (
    (1, 2),
    (2, 5),
    (5, 10),
    (10, 20),
    (20, 50),
    (50, 100),
    (100, 200),
    (200, 500),
    (500, 1000),
    (1000, 5000),
)


def _log_member_count_histogram(
    engine: ScriptEngine,
    concept_members: dict[str, set[str]],
    min_member_count: int,
) -> None:
    """打印概念成分数分布直方图，辅助判断历史残留并调 MIN_MEMBER_COUNT。

    过期/残留概念经 A 股池过滤后成分通常极少（1~2 个），直方图会让这部分长尾
    显形。``min_member_count`` 标记当前过滤线落在哪个桶。
    """
    counts: list[int] = [len(codes) for codes in concept_members.values()]
    if not counts:
        return

    total: int = len(counts)
    max_count: int = max(counts)
    bucket_counts: list[int] = [0] * len(_HISTOGRAM_BUCKETS)
    overflow: int = 0
    for n in counts:
        placed: bool = False
        for i, (lo, hi) in enumerate(_HISTOGRAM_BUCKETS):
            if lo <= n < hi:
                bucket_counts[i] += 1
                placed = True
                break
        if not placed:
            overflow += 1

    lines: list[str] = [f"概念成分数分布（共 {total} 个，最大 {max_count}）："]
    for (lo, hi), cnt in zip(_HISTOGRAM_BUCKETS, bucket_counts):
        marker: str = " <- MIN_MEMBER_COUNT 落在此桶" if lo <= min_member_count < hi else ""
        bar: str = "#" * min(cnt, 50)
        lines.append(f"  [{lo:>5}, {hi:>5}): {cnt:>5}  {bar}{marker}")
    if overflow:
        lines.append(f"  [5000,    ∞): {overflow:>5}  {'#' * min(overflow, 50)}")
    engine.write_log("\n".join(lines))


def _collect_snapshot(
    engine: ScriptEngine,
) -> tuple[list[tuple[str, str, str, str]], list[str]] | None:
    """从 xtquant 构建概念→成分完整去重快照；用户停止时返回 ``None``。

    返回 ``(members, concept_names)``：members 为
    ``(category, concept_name, stock_code, stock_name)`` 列表，concept_names 为
    去重后的概念名列表（用于日志统计）。
    """
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

    data_dir: Path | None = _get_data_dir()
    template_dir: Path = (data_dir / "Sector" / "Temple") if data_dir else None
    if not template_dir or not template_dir.is_dir():
        raise RuntimeError(
            "xtquant 本地板块分类目录不可用（Sector/Temple），无法判别概念分类归属"
        )

    categories: list[str] = _resolve_concept_categories(template_dir)
    if not categories:
        raise RuntimeError(
            f"未发现概念分类目录，请检查 Temple 子目录或 CONCEPT_CATEGORIES 配置；"
            f"已排除：{EXCLUDED_CATEGORIES}"
        )

    local_sources: dict[str, dict[str, list[Path]]] = _get_local_concept_sources(
        all_sectors, template_dir, categories, engine
    )

    # 同一概念名可能出现在多个分类下；汇总并记录其首个分类用于日志。
    concept_to_category: dict[str, str] = {}
    concept_to_paths: dict[str, list[Path]] = {}
    for category, cat_sectors in local_sources.items():
        for sector, paths in cat_sectors.items():
            concept_to_category.setdefault(sector, category)
            concept_to_paths.setdefault(sector, []).extend(paths)

    concepts: list[str] = sorted(concept_to_paths)
    if not concepts:
        raise RuntimeError(
            "筛选后没有概念板块，请检查分类目录或 EXCLUDED_CATEGORIES 配置"
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
        f"概念分类 {len(categories)} 个、概念 {len(concepts)} 个；"
        f"当前 A 股池 {len(active_codes)} 个"
    )

    # concept -> 成分集合（仅保留 A 股成分）。
    concept_members: dict[str, set[str]] = {}
    valid_concept_count: int = 0
    raw_relation_count: int = 0

    for index, concept in enumerate(concepts, start=1):
        if not engine.strategy_active:
            engine.write_log(
                f"同步已停止：已读取 {index - 1}/{len(concepts)} 个概念，数据库未修改"
            )
            return None

        raw_codes: set[str] = _read_local_sector_codes(concept_to_paths[concept])
        codes: set[str] = {
            code for code in raw_codes if code in active_codes
        }
        if codes:
            valid_concept_count += 1
            raw_relation_count += len(codes)
            concept_members[concept] = codes

        if index == 1 or index % PROGRESS_INTERVAL == 0 or index == len(concepts):
            engine.write_log(
                f"概念读取进度：{index}/{len(concepts)}，"
                f"有效概念 {valid_concept_count} 个、{raw_relation_count} 条原始关系"
            )

    if not valid_concept_count:
        raise RuntimeError("筛选后没有包含当前 A 股的概念，数据库未修改")

    # 成分数分布直方图：判断多少概念是成分极少的历史残留，据此调 MIN_MEMBER_COUNT。
    _log_member_count_histogram(engine, concept_members, MIN_MEMBER_COUNT)

    # 按成分数下限过滤过期/残留概念（Temple 累积型目录的历史长尾）。
    if MIN_MEMBER_COUNT > 1:
        before: int = len(concept_members)
        concept_members = {
            concept: codes
            for concept, codes in concept_members.items()
            if len(codes) >= MIN_MEMBER_COUNT
        }
        dropped: int = before - len(concept_members)
        engine.write_log(
            f"按 MIN_MEMBER_COUNT={MIN_MEMBER_COUNT} 过滤：剔除 {dropped} 个成分过少的概念，"
            f"剩余 {len(concept_members)} 个"
        )
        if not concept_members:
            raise RuntimeError(
                f"MIN_MEMBER_COUNT={MIN_MEMBER_COUNT} 过滤后无概念，请调低阈值"
            )

    # 收集所有涉及标的的唯一代码，统一取名称。
    involved_codes: list[str] = sorted(
        {code for codes in concept_members.values() for code in codes}
    )
    engine.write_log(f"开始读取 {len(involved_codes)} 个唯一标的名称")
    stock_names: dict[str, str] | None = _get_stock_names(engine, involved_codes)
    if stock_names is None:
        return None

    members: list[tuple[str, str, str, str]] = []
    for concept in sorted(concept_members):
        category: str = concept_to_category[concept]
        for code in sorted(concept_members[concept]):
            members.append((category, concept, code, stock_names.get(code, "")))

    engine.write_log(
        f"整理出 {len(concept_members)} 个有效概念、{len(members)} 条成分关系"
    )
    return members, sorted(concept_members)


def _ensure_table(sql_engine: SqlEngine) -> None:
    """不存在时创建概念成分快照表。"""
    sql_engine.execute(
        f"CREATE TABLE IF NOT EXISTS {CONCEPT_TABLE} ("
        "concept_category VARCHAR(64) NOT NULL, "
        "concept_name VARCHAR(128) NOT NULL, "
        "stock_code VARCHAR(64) NOT NULL, "
        "stock_name VARCHAR(128) NOT NULL DEFAULT '', "
        "PRIMARY KEY (concept_category, concept_name, stock_code)"
        ")"
    )


def _replace_snapshot(
    sql_engine: SqlEngine,
    driver: str,
    members: list[tuple[str, str, str, str]],
    engine: ScriptEngine,
) -> None:
    """在一个事务中精确替换快照，保证幂等性和失败回滚。"""
    placeholder: str = "?" if driver == "sqlite" else "%s"
    insert: str = (
        f"INSERT INTO {CONCEPT_TABLE} "
        f"(concept_category, concept_name, stock_code, stock_name) "
        f"VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})"
    )

    with sql_engine.transaction() as conn:
        conn.execute(f"DELETE FROM {CONCEPT_TABLE}")

        rows: list[tuple[Any, ...]] = [tuple(row) for row in members]
        total: int = len(rows)
        batch_count: int = (total + INSERT_BATCH_SIZE - 1) // INSERT_BATCH_SIZE
        for start in range(0, total, INSERT_BATCH_SIZE):
            batch: list[tuple[Any, ...]] = rows[start : start + INSERT_BATCH_SIZE]
            conn.executemany(insert, batch)
            finished: int = min(start + len(batch), total)
            batch_number: int = start // INSERT_BATCH_SIZE + 1
            engine.write_log(
                f"{driver.upper()} 写入进度：概念成分 {finished}/{total}，"
                f"第 {batch_number}/{batch_count} 批完成"
            )


def run(engine: ScriptEngine) -> None:
    """ScriptTrader 策略入口：读取 xtquant 概念板块快照并写入 SqlApp。"""
    sql_engine: SqlEngine | None = engine.main_engine.get_engine(APP_NAME)
    if sql_engine is None:
        raise RuntimeError(
            "概念同步脚本依赖 SqlApp，请先加载 SqlApp（script/run.py 中 add_app(SqlApp)）"
        )

    driver: str = getattr(sql_engine.database, "driver_name", "sqlite")
    engine.write_log(f"SqlApp 已就绪，数据库驱动：{driver}")

    snapshot: tuple[list[tuple[str, str, str, str]], list[str]] | None = (
        _collect_snapshot(engine)
    )
    if snapshot is None:
        return
    members, concept_names = snapshot

    if not engine.strategy_active:
        engine.write_log("同步已停止，数据库未修改")
        return

    _ensure_table(sql_engine)
    _replace_snapshot(sql_engine, driver, members, engine)
    engine.write_log(
        f"概念同步完成：{len(concept_names)} 个概念、{len(members)} 条成分关系"
    )
