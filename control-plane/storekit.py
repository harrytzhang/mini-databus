"""StoreKit 的 MySQL 部分（文档 18.6 / 18.11）：生成表名、建库建表、补字段、分区维护。

本模块所有函数都接收一个 info 字典（由 storage.get_storage_info 组装）：
    {
      "result_table_id": "591_order_event_clean",
      "physical_table_name": "mapleleaf_591.order_event_clean_591",
      "fields": [...结果表字段...],
      "storage_config": {"indexed_fields": [...], "storage_keys": [...], "has_unique_key": false},
      "expires_days": 7,
      "cluster": {"host", "port", "user", "password"},
    }

表结构：
- 系统字段 thedate / dtEventTime / dtEventTimeStamp / localTime + 业务字段
- 普通模式：按 thedate 做 RANGE 分区，一天一个分区，过期直接 DROP PARTITION（比 DELETE 快得多）
- 唯一键模式：增加 ____key 唯一索引，写入时 upsert 去重；表不分区，也就不会过期（文档 L10）
"""
import logging
import re
from datetime import date, timedelta

import pymysql

log = logging.getLogger(__name__)

# 结果表类型 -> MySQL 类型（文档 18.6.4）
TYPE_MAPPING = {
    "string": "text", "text": "text",
    "int": "int", "long": "bigint",
    "float": "float", "double": "double",
    "boolean": "tinyint",
}
# 这些字段不按结果表字段建列（文档 18.6.3 EXCEPT_FIELDS）
EXCEPT_FIELDS = {"timestamp", "offset", "thedate", "dteventtime", "dteventtimestamp", "localtime"}
SYSTEM_COLUMNS = [
    ("thedate", "int(11) NOT NULL"),
    ("dtEventTime", "varchar(32) NOT NULL"),
    ("dtEventTimeStamp", "bigint(20) NOT NULL"),
    ("localTime", "varchar(32) DEFAULT NULL"),
]
UNIQUE_KEY_COLUMN = "____key"
PARTITION_DAYS = 7              # 建表时预建几天的分区
MAX_PARTITION = "p30000101"     # 兜底分区：放超出已建分区范围的数据
AHEAD_DAYS = 3                  # 维护时提前建几天的分区


def physical_table_name(bk_biz_id, result_table_name):
    """文档 18.6.1：mapleleaf_{bk_biz_id}.{table_name}_{bk_biz_id}"""
    return f"mapleleaf_{bk_biz_id}.{result_table_name}_{bk_biz_id}"


def business_fields(fields):
    """入库的业务字段（去掉 timestamp 等平台字段），顺序就是 Java 端写入的列顺序"""
    return [f for f in fields if f["field_name"].lower() not in EXCEPT_FIELDS]


def connect(cluster, database=None):
    return pymysql.connect(host=cluster["host"], port=int(cluster["port"]), user=cluster["user"],
                           password=cluster["password"], database=database, charset="utf8mb4")


def _names(info):
    db, tb = info["physical_table_name"].split(".")
    return db, tb, f"`{db}`.`{tb}`"


# ------------------------- 建表 SQL -------------------------

def build_create_table_sql(info):
    _, _, table = _names(info)
    cfg = info["storage_config"]
    unique = cfg.get("has_unique_key")
    fields = business_fields(info["fields"])

    lines = [f"`{name}` {ddl}" for name, ddl in SYSTEM_COLUMNS]
    lines += [f"`{f['field_name']}` {TYPE_MAPPING[f['field_type']]} DEFAULT NULL" for f in fields]
    if unique:
        lines.append(f"`{UNIQUE_KEY_COLUMN}` varchar(128) NOT NULL")

    # 业务索引都是 “字段 + dtEventTimeStamp” 联合索引，文本字段用 30 字符前缀索引（文档 18.6.5）
    lines.append("KEY `ind_dtEventTimeStamp` (`dtEventTimeStamp`)")
    types = {f["field_name"]: f["field_type"] for f in fields}
    for name in cfg.get("indexed_fields", []):
        col = f"`{name}`(30)" if TYPE_MAPPING[types[name]] == "text" else f"`{name}`"
        lines.append(f"KEY `ind_{name}` ({col}, `dtEventTimeStamp`)")
    if unique:
        lines.append(f"UNIQUE KEY `ind_{UNIQUE_KEY_COLUMN}` (`{UNIQUE_KEY_COLUMN}`)")

    sql = f"CREATE TABLE IF NOT EXISTS {table} (\n  " + ",\n  ".join(lines)
    sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    if not unique:
        sql += "\n" + _partition_clause(date.today(), PARTITION_DAYS)
    return sql


def _day(d):
    return d.strftime("%Y%m%d")


def _partition_def(d):
    """p20261027 存放 thedate = 20261027 的数据"""
    return f"PARTITION p{_day(d)} VALUES LESS THAN ({_day(d + timedelta(days=1))})"


def _partition_clause(start, days):
    parts = [_partition_def(start + timedelta(days=i)) for i in range(days)]
    parts.append(f"PARTITION {MAX_PARTITION} VALUES LESS THAN MAXVALUE")
    return "PARTITION BY RANGE (`thedate`) (\n  " + ",\n  ".join(parts) + "\n)"


# ------------------------- prepare：建库建表 / 加列（文档 18.6.2）-------------------------

def _base_type(t):
    """'bigint(20) unsigned' -> 'bigint'"""
    return re.split(r"[\s(]", t.strip().lower(), maxsplit=1)[0]


def prepare(info):
    """入库任务启动前调用：表不存在就建；存在就补齐新增字段。只加列，不删列也不改类型。"""
    db, tb, table = _names(info)
    conn = connect(info["cluster"])
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` DEFAULT CHARSET utf8mb4")
            cur.execute(f"SHOW TABLES IN `{db}` LIKE %s", (tb,))
            if cur.fetchone() is None:
                cur.execute(build_create_table_sql(info))
                log.info("创建表 %s", table)
                return {"created": True, "added_columns": []}

            cur.execute(f"DESC {table}")
            existing = {row[0]: _base_type(row[1]) for row in cur.fetchall()}
            added = []
            for f in business_fields(info["fields"]):
                want = TYPE_MAPPING[f["field_type"]]
                name = f["field_name"]
                if name not in existing:
                    cur.execute(f"ALTER TABLE {table} ADD `{name}` {want} DEFAULT NULL")
                    added.append(name)
                    log.info("表 %s 新增字段 %s", table, name)
                elif existing[name] != want:
                    raise RuntimeError(f"字段 {name} 类型不一致：表中是 {existing[name]}，结果表是 {want}")
        conn.commit()
        return {"created": False, "added_columns": added}
    finally:
        conn.close()


# ------------------------- maintain：分区维护（文档 18.11）-------------------------

def maintain(info):
    """预建未来几天的分区，删除过期分区。唯一键模式的表不是分区表，直接跳过。"""
    _, _, table = _names(info)
    expires = int(info.get("expires_days", 7))
    today = date.today()
    added, dropped = [], []

    conn = connect(info["cluster"])
    try:
        with conn.cursor() as cur:
            cur.execute(f"SHOW CREATE TABLE {table}")
            parts = set(re.findall(r"PARTITION (p\d+)", cur.fetchone()[1]))
            if not parts:
                return {"skipped": "非分区表"}

            # 1. 预建分区：把 MAXVALUE 兜底分区拆成 “新的一天 + 兜底分区”
            for i in range(AHEAD_DAYS + 1):
                d = today + timedelta(days=i)
                if f"p{_day(d)}" not in parts:
                    cur.execute(f"ALTER TABLE {table} REORGANIZE PARTITION {MAX_PARTITION} INTO "
                                f"({_partition_def(d)}, PARTITION {MAX_PARTITION} VALUES LESS THAN MAXVALUE)")
                    added.append(f"p{_day(d)}")

            # 2. 删除过期分区：DROP PARTITION 瞬间释放空间
            deadline = _day(today - timedelta(days=expires))
            for p in sorted(parts):
                if p != MAX_PARTITION and p[1:] < deadline:
                    cur.execute(f"ALTER TABLE {table} DROP PARTITION {p}")
                    dropped.append(p)
        conn.commit()
    finally:
        conn.close()
    if added or dropped:
        log.info("%s 分区维护：新增 %s，删除 %s", table, added, dropped)
    return {"added": added, "dropped": dropped}


def rename_to_delete(info):
    """删除存储时不直接删表，改名为 _to_delete 后缀，留出反悔时间（文档 18.5.4）"""
    db, tb, table = _names(info)
    conn = connect(info["cluster"])
    try:
        with conn.cursor() as cur:
            cur.execute(f"SHOW TABLES IN `{db}` LIKE %s", (tb,))
            if cur.fetchone():
                cur.execute(f"RENAME TABLE {table} TO `{db}`.`{tb}_to_delete`")
        conn.commit()
    finally:
        conn.close()
