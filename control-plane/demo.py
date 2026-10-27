"""端到端演示：调用控制面接口，完成 “接入 -> 清洗 -> 入库”，然后灌模拟数据，观察 MySQL 行数。

前提：Kafka、MySQL、Java Worker、Python API 都已启动。
用法：python demo.py [消息条数，默认 100000]
"""
import json
import os
import random
import sys
import time
from datetime import datetime

import pymysql
import requests
from confluent_kafka import Producer

import kafka_admin
import settings

API = os.getenv("API_URL", f"http://localhost:{settings.API_PORT}")
EXTERNAL_KAFKA = os.getenv("EXTERNAL_KAFKA", settings.OUTER_KAFKA)   # 外部业务方的 Kafka
EXTERNAL_TOPIC = "order_event"
BIZ_ID = 591
ITEMS = ["book", "pen", "phone", "cup", "shoes", "bag", "watch", "lamp"]

# 清洗规则：一条订单包含多个商品，iterate 节点把它展开成多行
JSON_CONFIG = {
    "time_field": "event_time",
    "time_format": "yyyy-MM-dd HH:mm:ss",
    "timezone": 8,
    "rule": {
        "type": "json",
        "next": [
            {"type": "assign", "fields": [
                {"key": "order_id", "as": "order_id"},
                {"key": "user_id", "as": "user_id"},
                {"key": "event_time", "as": "event_time"},
            ]},
            {"type": "access", "key": "items", "next": [
                {"type": "iterate", "next": [
                    {"type": "assign", "fields": [
                        {"key": "item", "as": "item"},
                        {"key": "amount", "as": "amount"},
                    ]},
                ]},
            ]},
        ],
    },
}

FIELDS = [
    {"field_name": "order_id", "field_type": "string", "field_alias": "订单ID"},
    {"field_name": "user_id", "field_type": "long", "field_alias": "用户ID"},
    {"field_name": "item", "field_type": "string", "field_alias": "商品"},
    {"field_name": "amount", "field_type": "double", "field_alias": "金额"},
    {"field_name": "event_time", "field_type": "string", "field_alias": "事件时间"},
]


def call(method, path, payload=None):
    resp = requests.request(method, API + path, json=payload, timeout=120)
    data = resp.json()
    print(f"{method} {path} -> {resp.status_code} {json.dumps(data, ensure_ascii=False)[:300]}")
    if not data.get("result"):
        sys.exit(1)
    return data["data"]


def mock_order(i):
    return {
        "order_id": f"O{i:012d}",
        "user_id": random.randint(1, 1_000_000),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "items": [{"item": random.choice(ITEMS), "amount": round(random.uniform(1, 500), 2)}
                  for _ in range(random.randint(1, 3))],
    }


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 100000

    print("== 0. 准备外部 topic（模拟业务方已有的 Kafka）==")
    kafka_admin.ensure_topic(EXTERNAL_KAFKA, EXTERNAL_TOPIC, settings.TOPIC_PARTITIONS)

    print("\n== 1. 创建接入（自动启动 Puller）==")
    raw = call("POST", "/v3/access/deploy_plan/", {
        "data_scenario": "queue", "bk_biz_id": BIZ_ID, "bk_username": "admin",
        "access_raw_data": {"raw_data_name": "order_event", "raw_data_alias": "订单事件", "data_encoding": "UTF-8"},
        "access_conf_info": {"resource": {"type": "kafka", "scope": [{
            "master": EXTERNAL_KAFKA, "topic": EXTERNAL_TOPIC, "group": "bkbase_order_puller",
            "tasks": 3, "auto_offset_reset": "earliest",
        }]}},
    })

    print("\n== 2. 创建清洗配置并启动 ==")
    rt = call("POST", "/v3/databus/scenarios/setup_clean/", {
        "raw_data_id": raw["raw_data_id"], "bk_biz_id": BIZ_ID,
        "result_table_name": "order_event_clean", "result_table_name_alias": "订单事件清洗表",
        "clean_config_name": "订单事件清洗", "json_config": JSON_CONFIG, "fields": FIELDS,
    })

    print("\n== 3. 添加 MySQL 入库（自动建表并启动入库任务）==")
    st = call("POST", "/v3/databus/data_storages/", {
        "raw_data_id": raw["raw_data_id"], "data_type": "clean", "result_table_name": "order_event_clean",
        "storage_type": "mysql", "storage_cluster": "mysql-default", "expires": "7",
        "fields": [
            {"field_name": "order_id", "is_index": True, "is_key": True},
            {"field_name": "user_id", "is_index": True},
            {"field_name": "item", "is_key": True},
        ],
        "config": {"has_unique_key": False},
    })

    print(f"\n== 4. 向外部 topic 灌 {count} 条订单 ==")
    producer = Producer({"bootstrap.servers": EXTERNAL_KAFKA, "linger.ms": 20, "compression.type": "lz4"})
    start = time.time()
    for i in range(count):
        while True:
            try:
                producer.produce(EXTERNAL_TOPIC, json.dumps(mock_order(i)).encode())
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    producer.flush()
    print(f"发送完成，{count / (time.time() - start):.0f} 条/秒")

    print(f"\n== 5. 观察 MySQL 表 {st['physical_table_name']} 的行数 ==")
    cluster = settings.STORAGE_CLUSTERS["mysql-default"]
    conn = pymysql.connect(host=cluster["host"], port=cluster["port"], user=cluster["user"],
                           password=cluster["password"], autocommit=True)
    db, tb = st["physical_table_name"].split(".")
    last = 0
    for _ in range(30):
        time.sleep(2)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{db}`.`{tb}`")
            n = cur.fetchone()[0]
        print(f"行数 {n}（+{(n - last) / 2:.0f} 行/秒）")
        if n == last and n > 0:
            break
        last = n

    print("\n== 6. 任务状态与指标 ==")
    for task in call("GET", f"/v3/databus/tasks/?processing_id={rt['result_table_id']}"):
        print(task["connector_task_name"], json.dumps(task["worker"], ensure_ascii=False))


if __name__ == "__main__":
    main()
