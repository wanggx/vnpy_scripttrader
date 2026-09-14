"""A 股标的池公共常量。

所有 ``script/`` 脚本统一从此导入，避免市场过滤口径（沪深 A 股、过滤北交所）
散落多处定义导致漂移。本模块无副作用，不 import xtquant / bigqmt_xtdata，
MiniQMT 与大 QMT 两套脚本均可安全引用。
"""

# xtquant 中沪深 A 股板块名，成分本身即仅含沪市(.SH)、深市(.SZ) A 股，
# 不含北交所。VALID_MARKETS 作为防御性二次过滤，杜绝个别混入的其他市场代码。
PRIMARY_SECTOR: str = "沪深A股"

# PRIMARY_SECTOR 取不到成分时的回退板块（留空即不回退；"沪深A股"为 xtquant
# 标准板块，正常均存在）。
FALLBACK_SECTORS: tuple[str, ...] = ()

# 仅保留沪深 A 股，过滤北交所（.BJ）及其他市场标的。
VALID_MARKETS: tuple[str, ...] = (".SH", ".SZ")
