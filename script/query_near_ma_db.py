"""只读查询 SqlApp 的 ``stock_near_ma`` 表，用于复盘最近入选记录。

用法（在 script/ 目录下）：
    python query_near_ma_db.py            # 最近 7 天全部记录摘要
    python query_near_ma_db.py 600021.SH  # 只看某只标的的历史入选
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pymysql

SETTING_PATH = Path.home() / ".vntrader" / "vt_setting.json"


def main() -> None:
    """连接 SqlApp 配置的库并打印记录。"""
    cfg = json.loads(SETTING_PATH.read_text(encoding="utf-8"))
    conn = pymysql.connect(
        host=cfg["sqlapp.host"],
        port=int(cfg["sqlapp.port"]),
        user=cfg["sqlapp.user"],
        password=cfg["sqlapp.password"],
        database=cfg["sqlapp.database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            if len(sys.argv) > 1 and sys.argv[1] == "--tables":
                cur.execute("SHOW TABLES")
                for row in cur.fetchall():
                    print(next(iter(row.values())))
                return
            if len(sys.argv) > 1:
                code = sys.argv[1]
                cur.execute(
                    "SELECT trade_date, code, name, close, high52w, score, "
                    "near_dates, near_days, near_values, sector_name "
                    "FROM stock_near_ma WHERE code = %s ORDER BY trade_date DESC",
                    (code,),
                )
                rows = cur.fetchall()
                print(f"{code} 共 {len(rows)} 行：")
                for r in rows:
                    print(
                        f"  {r['trade_date']} {r['name']} close={r['close']} "
                        f"high52w={r['high52w']} score={r['score']} "
                        f"near={r['near_dates']} days={r['near_days']} "
                        f"ma={r['near_values']} [{r['sector_name']}]"
                    )
            else:
                cur.execute(
                    "SELECT trade_date, COUNT(*) AS n, MAX(score) AS top_score, "
                    "MIN(score) AS low_score FROM stock_near_ma "
                    "GROUP BY trade_date ORDER BY trade_date DESC LIMIT 15"
                )
                for r in cur.fetchall():
                    print(
                        f"{r['trade_date']} 行数={r['n']} "
                        f"最高分={r['top_score']} 最低分={r['low_score']}"
                    )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
