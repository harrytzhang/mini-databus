"""元数据库（对应文档第 4 章数据模型）。

原平台用 Django ORM + MySQL，这里用 Python 自带的 sqlite3，表结构保留核心字段：

    access_raw_data         原始数据源，id 即 Data ID（文档 4.2）
    access_resource_info    接入资源配置 scope（文档 4.3）
    databus_clean           清洗配置 + 结果表字段（文档 4.6，结果表字段原本存在 MetaApi）
    storage_result_table    结果表的存储关联（StoreKit，文档 18.4）
    databus_connector_task  下发到 Worker 的任务记录（文档 4.5）
"""
import sqlite3
from contextlib import closing

import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS access_raw_data (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bk_biz_id       INTEGER NOT NULL,
    raw_data_name   TEXT    NOT NULL,
    raw_data_alias  TEXT,
    data_scenario   TEXT    NOT NULL,
    data_encoding   TEXT    NOT NULL,
    topic           TEXT    NOT NULL,
    created_by      TEXT,
    created_at      TEXT    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (bk_biz_id, raw_data_name)
);

CREATE TABLE IF NOT EXISTS access_resource_info (
    raw_data_id     INTEGER PRIMARY KEY,
    resource        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS databus_clean (
    processing_id            TEXT PRIMARY KEY,
    raw_data_id              INTEGER NOT NULL,
    bk_biz_id                INTEGER NOT NULL,
    result_table_name        TEXT    NOT NULL,
    result_table_name_alias  TEXT,
    clean_config_name        TEXT,
    json_config              TEXT    NOT NULL,
    fields                   TEXT    NOT NULL,
    status                   TEXT    NOT NULL DEFAULT 'stopped',
    updated_at               TEXT    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS storage_result_table (
    result_table_id      TEXT NOT NULL,
    storage_type         TEXT NOT NULL,
    cluster_name         TEXT NOT NULL,
    physical_table_name  TEXT NOT NULL,
    expires              TEXT NOT NULL,
    storage_config       TEXT NOT NULL,
    PRIMARY KEY (result_table_id, storage_type)
);

CREATE TABLE IF NOT EXISTS databus_connector_task (
    connector_task_name  TEXT PRIMARY KEY,
    processing_id        TEXT NOT NULL,
    task_type            TEXT NOT NULL,
    source               TEXT NOT NULL,
    sink                 TEXT NOT NULL,
    status               TEXT NOT NULL,
    updated_at           TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

# Data ID 从 10001 开始，和文档示例一致
INIT_DATA_ID = """
INSERT INTO sqlite_sequence (name, seq)
SELECT 'access_raw_data', 10000
WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'access_raw_data');
"""


def connect():
    conn = sqlite3.connect(settings.META_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init():
    with closing(connect()) as conn:
        conn.executescript(SCHEMA)
        conn.executescript(INIT_DATA_ID)
        conn.commit()


def execute(sql, params=()):
    """执行写操作，返回自增 id"""
    with closing(connect()) as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid


def query_one(sql, params=()):
    with closing(connect()) as conn:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None


def query_all(sql, params=()):
    with closing(connect()) as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
