"""清洗配置与清洗任务（对应 databus/clean.py + CleanShipper，文档第 8、9 章）。

- create_clean   只创建配置，不启动（文档 L7：容易误以为已开始清洗）
- start_clean    分区校验 -> 按 consume_position 调整消费位点 -> 生成 TransformSinkConnector 配置 -> 下发到 Worker
- setup_clean    场景化接口：创建 + 启动
- update_clean   更新配置；任务运行中会重新下发让配置生效（原平台用 ZK 通知热更新，文档第 13 章）

两种启动方式（consume_position，对应原平台 CleanShipper 的同名参数）：
- continue（默认）从上次停止的位置继续消费，停止期间积压的数据会被补齐
- tail            忽略停止期间的数据，从调用接口时刻 Raw Topic 的末尾开始消费

和原平台一样，Java 侧没有新增参数：控制面在下发任务【之前】修改 Java 启动时读取的状态。
本项目的 CleanTask 从消费组 connect-{connector 名} 已提交的 offset 恢复，所以 tail 的做法是
先停任务，再把该消费组的 offset 提交为 Raw Topic 各分区的 LEO（类似原平台 Pulsar 的 skip_all），
不需要像原平台 Kafka 方案那样往结果 topic 写标记消息。
"""
import json
import re

import access
import kafka_admin
import meta_db
import settings
import worker

NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,49}$")
FIELD_TYPES = {"string", "text", "int", "long", "float", "double", "boolean"}
# 平台保留字段：由平台生成或用于存储，用户不能定义（文档 8.2.2 / 18.6.3）
RESERVED_FIELDS = {"timestamp", "offset", "thedate", "dteventtime", "dteventtimestamp", "localtime", "____key"}
NODE_TYPES = {"json", "split", "regex", "access", "iterate", "assign"}
CONSUME_POSITIONS = ("continue", "tail")


def rt_id_of(bk_biz_id, result_table_name):
    return f"{bk_biz_id}_{result_table_name}"


def inner_topic(rt_id):
    """文档 6.4：清洗结果 topic = table_{rt_id}"""
    return f"table_{rt_id}"


def clean_name(rt_id):
    return f"clean-{rt_id}"


def clean_group(rt_id):
    """CleanTask 的消费组，与 CleanTask.consumerConfig() 中的 group.id 保持一致"""
    return f"connect-{clean_name(rt_id)}"


def validate_consume_position(value):
    value = value or "continue"
    if value not in CONSUME_POSITIONS:
        raise ValueError(f"consume_position 取值不合法：{value}，可选 {list(CONSUME_POSITIONS)}")
    return value


# ------------------------- 校验 -------------------------

def validate_fields(fields):
    if not fields:
        raise ValueError("fields 不能为空")
    result, seen = [], set()
    for i, f in enumerate(fields, start=1):
        name, ftype = f.get("field_name", ""), f.get("field_type", "")
        if not NAME_RE.match(name):
            raise ValueError(f"字段名不合法：{name}")
        if name.lower() in RESERVED_FIELDS:
            raise ValueError(f"字段名是平台保留字段：{name}")
        if name.lower() in seen:
            raise ValueError(f"字段重复：{name}")
        if ftype not in FIELD_TYPES:
            raise ValueError(f"字段 {name} 的类型 {ftype} 不支持，可选 {sorted(FIELD_TYPES)}")
        seen.add(name.lower())
        result.append({
            "field_name": name,
            "field_type": ftype,
            "field_alias": f.get("field_alias") or name,
            "is_dimension": bool(f.get("is_dimension")),
            "field_index": i,
        })
    # 文档 8.2.2：平台自动追加 timestamp 字段（数据时间，秒）
    result.append({"field_name": "timestamp", "field_type": "long", "field_alias": "数据时间戳",
                   "is_dimension": False, "field_index": len(result) + 1})
    return result


def validate_json_config(json_config):
    """json_config 可以是 JSON 字符串或对象；必须包含 rule、time_field"""
    conf = json.loads(json_config) if isinstance(json_config, str) else dict(json_config or {})
    if not conf.get("time_field"):
        raise ValueError("json_config 缺少 time_field")
    conf.setdefault("time_format", "yyyy-MM-dd HH:mm:ss")
    conf.setdefault("timezone", 8)
    _check_node(conf.get("rule"))
    return conf


def _check_node(node):
    if not isinstance(node, dict) or node.get("type") not in NODE_TYPES:
        raise ValueError(f"规则节点不合法：{node}，type 可选 {sorted(NODE_TYPES)}")
    if node["type"] == "split" and not node.get("delimiter"):
        raise ValueError("split 节点需要 delimiter")
    if node["type"] == "regex" and not (node.get("pattern") and node.get("keys")):
        raise ValueError("regex 节点需要 pattern 和 keys")
    if node["type"] == "access" and node.get("key") is None:
        raise ValueError("access 节点需要 key")
    if node["type"] == "assign" and not node.get("fields"):
        raise ValueError("assign 节点需要 fields")
    for child in node.get("next", []):
        _check_node(child)


# ------------------------- 接口实现 -------------------------

def create_clean(params):
    raw = access.get_raw_data(params["raw_data_id"])
    biz = int(params["bk_biz_id"])
    name = params.get("result_table_name", "")
    if not NAME_RE.match(name):
        raise ValueError("result_table_name 只能包含字母、数字、下划线，且以字母开头")
    rt_id = rt_id_of(biz, name)
    if meta_db.query_one("SELECT 1 FROM databus_clean WHERE processing_id=?", (rt_id,)):
        raise ValueError(f"结果表 {rt_id} 已存在")
    fields = validate_fields(params.get("fields"))
    conf = validate_json_config(params.get("json_config"))

    # 建 inner topic，分区数不小于 Raw Topic（文档 9.2）
    raw_partitions = kafka_admin.partition_count(settings.OUTER_KAFKA, raw["topic"])
    kafka_admin.ensure_topic(settings.INNER_KAFKA, inner_topic(rt_id), max(raw_partitions, settings.TOPIC_PARTITIONS))

    meta_db.execute(
        "INSERT INTO databus_clean (processing_id, raw_data_id, bk_biz_id, result_table_name, "
        "result_table_name_alias, clean_config_name, json_config, fields) VALUES (?,?,?,?,?,?,?,?)",
        (rt_id, raw["id"], biz, name, params.get("result_table_name_alias"), params.get("clean_config_name"),
         json.dumps(conf, ensure_ascii=False), json.dumps(fields, ensure_ascii=False)),
    )
    return {"processing_id": rt_id, "result_table_id": rt_id}


def update_clean(rt_id, params):
    clean = get_clean(rt_id)
    fields = validate_fields(params["fields"]) if params.get("fields") else clean["fields"]
    conf = validate_json_config(params["json_config"]) if params.get("json_config") else clean["json_config"]
    meta_db.execute(
        "UPDATE databus_clean SET json_config=?, fields=?, updated_at=CURRENT_TIMESTAMP WHERE processing_id=?",
        (json.dumps(conf, ensure_ascii=False), json.dumps(fields, ensure_ascii=False), rt_id),
    )
    if clean["status"] == "running":
        start_clean(rt_id)   # 重新下发 = 让运行中的任务用上新配置
    return {"processing_id": rt_id}


def setup_clean(params):
    consume_position = validate_consume_position(params.get("consume_position"))
    result = create_clean(params)
    start_clean(result["result_table_id"], consume_position)
    return result


def get_clean(rt_id):
    row = meta_db.query_one("SELECT * FROM databus_clean WHERE processing_id=?", (rt_id,))
    if row is None:
        raise LookupError(f"结果表 {rt_id} 不存在")
    row["fields"] = json.loads(row["fields"])
    row["json_config"] = json.loads(row["json_config"])
    return row


# ------------------------- 清洗任务（CleanShipper）-------------------------

def build_clean_conf(clean, raw, tasks_max):
    """文档 9.4：Kafka Clean 配置。字段和规则直接放进配置（原平台由 Java 任务调 API 拉取）"""
    rt_id = clean["processing_id"]
    return {
        "connector.class": "TransformSinkConnector",
        "rt.id": rt_id,
        "processing.id": rt_id,
        "db.dataid": raw["id"],
        "topics": raw["topic"],
        "source.bootstrap.servers": settings.OUTER_KAFKA,
        "tasks.max": tasks_max,
        "producer.bootstrap.servers": settings.INNER_KAFKA,
        "sink.topic": inner_topic(rt_id),
        "producer.records.in.msg": settings.RECORDS_IN_MSG,
        "data.encoding": raw["data_encoding"],
        "bad.data.topic": settings.BAD_DATA_TOPIC,
        "rt.fields": json.dumps(clean["fields"], ensure_ascii=False),
        "etl.conf": json.dumps(clean["json_config"], ensure_ascii=False),
    }


def start_clean(rt_id, consume_position="continue"):
    consume_position = validate_consume_position(consume_position)
    clean = get_clean(rt_id)
    raw = access.get_raw_data(clean["raw_data_id"])

    # 分区校验：inner 分区不够就扩容（文档 9.2）；并发度 = Raw Topic 分区数
    raw_partitions = kafka_admin.partition_count(settings.OUTER_KAFKA, raw["topic"])
    if raw_partitions == 0:
        raise RuntimeError(f"Raw Topic {raw['topic']} 不存在")
    kafka_admin.ensure_topic(settings.INNER_KAFKA, inner_topic(rt_id), raw_partitions)
    if settings.BAD_DATA_TOPIC:
        kafka_admin.ensure_topic(settings.INNER_KAFKA, settings.BAD_DATA_TOPIC, 1)

    if consume_position == "tail":
        _seek_to_tail(rt_id, raw["topic"], raw_partitions)
    # continue：什么都不做，CleanTask 从消费组已提交的 offset 继续；从未消费过时按 earliest 从头开始

    worker.start_task(
        clean_name(rt_id), build_clean_conf(clean, raw, raw_partitions),
        processing_id=rt_id, task_type="clean",
        source=f"kafka#{raw['topic']}", sink=f"kafka#{inner_topic(rt_id)}",
    )
    meta_db.execute("UPDATE databus_clean SET status='running' WHERE processing_id=?", (rt_id,))
    return {"consume_position": consume_position}


def _seek_to_tail(rt_id, raw_topic, raw_partitions):
    """tail：先停任务（运行中的消费者会覆盖我们提交的 offset，broker 也会拒绝提交），
    再把消费组位点推到 Raw Topic 末尾。跳过的截止点是调用接口的时刻，此后到达的数据会被消费。"""
    stop_clean(rt_id)   # 未运行时 Worker 返回 404，视为成功
    kafka_admin.seek_group_to_end(settings.OUTER_KAFKA, clean_group(rt_id), raw_topic, raw_partitions)


def stop_clean(rt_id):
    worker.stop_task(clean_name(rt_id))
    meta_db.execute("UPDATE databus_clean SET status='stopped' WHERE processing_id=?", (rt_id,))
