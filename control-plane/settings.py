"""控制面配置。全部可以用环境变量覆盖（原平台的 dataapi_settings.py 也是从环境变量读取）。"""
import os

# ---------- 平台消息通道（文档 6.2 / 8.2.1）----------
# outer 通道存放 Raw Topic，inner 通道存放清洗结果 table_{rt_id}；学习时可以是同一个 Kafka
OUTER_KAFKA = os.getenv("OUTER_KAFKA", "localhost:9092")
INNER_KAFKA = os.getenv("INNER_KAFKA", OUTER_KAFKA)
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "6"))     # 新建 topic 的分区数 = 最大并发度
TOPIC_REPLICATION = int(os.getenv("TOPIC_REPLICATION", "1"))
BAD_DATA_TOPIC = os.getenv("BAD_DATA_TOPIC", "bad_data")       # 清洗失败的坏数据（文档第 14 章）

# ---------- Java 数据面 Worker（相当于 Kafka Connect 集群的 REST 地址）----------
WORKER_URL = os.getenv("WORKER_URL", "http://localhost:8083")

# ---------- 元数据库 ----------
# 原平台用 MySQL 存接入 / 清洗 / 任务配置；这里用 SQLite，不需要额外部署
META_DB = os.getenv("META_DB", "meta.db")

# ---------- 存储集群（原平台存在 StoreKit 的集群配置表里）----------
STORAGE_CLUSTERS = {
    "mysql-default": {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "root123"),
    },
}

# ---------- 入库任务参数 ----------
MYSQL_BATCH_SIZE = int(os.getenv("MYSQL_BATCH_SIZE", "2000"))  # 每批写入行数（文档 18.7.4）
RECORDS_IN_MSG = int(os.getenv("RECORDS_IN_MSG", "100"))       # 清洗输出每条消息包含的行数

# ---------- API 服务 ----------
API_PORT = int(os.getenv("API_PORT", "8000"))
MAINTAIN_HOUR = int(os.getenv("MAINTAIN_HOUR", "7"))           # 每天几点做分区维护（原平台 07:30）
