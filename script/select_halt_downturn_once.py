"""全市场止跌形态扫描：启动后立刻跑一轮，跑完结束。

逻辑与 ``select_halt_downturn_xtquant.py`` 完全相同（沪深A股、严格档小十字星+
缩量、打分、入库），只是不做 16:30 调度循环。适合手动验证或临时补跑当天结果。

与定时版的区别：定时版排在 16:00 的 ``select_near_ma_*`` 之后，默认直接读本地
数据不补数；这里手动跑通常没有前置补数，所以默认**开启补数**（较慢，要先探一轮
全市场本地覆盖）。当天已经补过数据、只想快速重跑时，把下面的 ``DOWNLOAD_MISSING``
改成 False；也可以在 ScriptTrader 的「编辑脚本」界面里把 ``download_missing`` 填 False。

**注意 ScriptTrader 的参数机制**：``run()`` 里除 ``engine`` 外的参数都会被当作「脚本
参数」列到界面里，值持久化在 ``~/.vntrader/script_trader_setting.json``，运行时按名字
传回。界面是按「存储值的类型」决定控件的，且值走文本框回传，所以配置里的布尔参数
可能落成**字符串**（``"false"``）——直接拿它做 ``if`` 判断会得到真值（非空字符串恒为真）。
本脚本用 ``_coerce_bool`` 显式解析文本，避免这种"填了 False 却照样补数"的坑。

日常定时请用 ``select_halt_downturn_xtquant.py``。
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

import select_halt_downturn_xtquant as halt

if TYPE_CHECKING:
    from vnpy_scripttrader.engine import ScriptEngine


# 是否先探本地覆盖并补缺目标日数据（默认开：手动/补跑一般没有前置补数）。
# 含义同定时版 ``select_halt_downturn_xtquant.DOWNLOAD_MISSING``；当天已补过数据时
# 改成 False，可跳过一整轮全市场 RPC 探活。也会作为 ``run()`` 的默认值出现在
# ScriptTrader 的「脚本参数」界面里（界面填的值优先，见下面的 ``_coerce_bool``）。
DOWNLOAD_MISSING: bool = True

# 文本形式的布尔值：ScriptTrader 的脚本参数经文本框回传，bool 参数可能落成字符串。
_TRUE_TEXT: frozenset[str] = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE_TEXT: frozenset[str] = frozenset({"0", "false", "f", "no", "n", "off"})


def _coerce_bool(value: object, default: bool) -> bool:
    """把界面/配置文件传来的值解析成 bool。

    ``bool("false")`` 是 True（非空字符串恒为真），所以不能直接 ``if value``：配置里把
    ``download_missing`` 存成字符串 ``"false"`` 时，会变成"填了不补数却照样补数"。
    ``None`` / 空串 → ``default``（模块常量）；无法识别 → 抛 ValueError（宁可明确报错，
    也不要静默用错口径跑一轮全市场）。
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text: str = str(value).strip().lower()
    if not text:
        return default
    if text in _TRUE_TEXT:
        return True
    if text in _FALSE_TEXT:
        return False
    raise ValueError(
        f"无法把 {value!r} 解析成布尔值：请填 True 或 False（大小写不敏感），留空用默认值"
    )


def run(engine: ScriptEngine, download_missing: bool = DOWNLOAD_MISSING) -> None:
    """ScriptTrader 入口：立即执行一轮全市场止跌形态扫描后退出。

    ``download_missing`` 默认就是模块常量 ``DOWNLOAD_MISSING``；ScriptTrader 会把它列在
    「编辑脚本」界面里，运行时传入界面填的值（不填/填空则用默认值）。
    """
    try:
        effective: bool = _coerce_bool(download_missing, DOWNLOAD_MISSING)
    except ValueError as exc:
        engine.write_log(f"参数不合法：{exc}")
        raise
    engine.write_log(
        "全市场止跌形态扫描立即执行一轮（小十字星+极致缩量），跑完即结束；"
        # !r：字符串会带引号（'false'），一眼看出是不是被界面存成了字符串
        f"参数 download_missing：传入 {download_missing!r}，生效 {effective}"
        f"（模块常量 DOWNLOAD_MISSING={DOWNLOAD_MISSING}）；本轮数据准备："
        + ("先探本地覆盖并补缺当日数据" if effective else "直接读本地数据（不补数）")
    )
    try:
        # 手动/补跑场景通常没有前置补数，默认打开补数开关（定时版默认不补数）。
        halt._run_once(  # noqa: SLF001 - 复用主脚本单轮逻辑
            engine, download_missing=effective
        )
    except Exception:  # noqa: BLE001 - 异常打日志后仍正常结束脚本
        engine.write_log(f"本轮执行异常：\n{traceback.format_exc()}")
    engine.write_log("全市场止跌形态扫描本轮结束")
