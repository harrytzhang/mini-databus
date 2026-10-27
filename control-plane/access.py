"""Queue 接入（对应 access 模块：deploy_plan 接口 + QueueAccess + QueuePuller，文档第 5.2、6、7 章）。

创建接入的流程：
    校验参数 -> 建 Raw Topic -> 写 Raw Data（得到 Data ID）-> 保存 scope -> 创建并启动 Puller
"""
import json
import re

import kafka_admin
import meta_db
import settings
import worker

SUPPORTED_RESOURCE_TYPES = ("kafka",)
NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,49}$")
BROKER_RE = re.compile(r"^[\w.\-]+:\d+$")
SASL_KEYS = ("security_protocol", "sasl_mechanism", "user", "password")
MASK = "******"


def raw_topic_name(raw_data_name, bk_biz_id):
    """文档 6.4：Raw Topic = {raw_data_name}{bk_biz_id}"""
    return f"{raw_data_name}{bk_biz_id}"


def puller_name(raw_data_id):
    return f"puller-{raw_data_id}"


# ------------------------- 参数校验（QueueAccess.valid_access_param）-------------------------

def validate_scope(access_conf_info):
    resource = (access_conf_info or {}).get("resource") or {}
    if resource.get("type") not in SUPPORTED_RESOURCE_TYPES:
        raise ValueError(f"resource.type 只支持 {SUPPORTED_RESOURCE_TYPES}")
    scopes = resource.get("scope") or []
    if len(scopes) != 1:
        raise ValueError("一个 Data ID 只支持一个 scope（文档 L3）")

    scope = dict(scopes[0])
    for key in ("master", "topic", "group"):
        if not scope.get(key):
            raise ValueError(f"scope.{key} 不能为空")
    # 改进 L2：支持逗号分隔的多个 broker 地址
    for addr in scope["master"].split(","):
        if not BROKER_RE.match(addr.strip()):
            raise ValueError(f"broker 地址格式应为 host:port：{addr}")
    scope["tasks"] = int(scope.get("tasks") or 1)
    scope["auto_offset_reset"] = scope.get("auto_offset_reset") or "latest"
    if scope["auto_offset_reset"] not in ("latest", "earliest"):
        raise ValueError("auto_offset_reset 只能是 latest 或 earliest")
    if scope.get("use_sasl"):
        missing = [k for k in SASL_KEYS if not scope.get(k)]
        if missing:
            raise ValueError(f"启用 SASL 时必须填写：{missing}")
    return scope


# ------------------------- 接口实现 -------------------------

def create_deploy_plan(params):
    if params.get("data_scenario") != "queue":
        raise ValueError("本项目只实现 data_scenario=queue")
    raw = params.get("access_raw_data") or {}
    name = raw.get("raw_data_name", "")
    if not NAME_RE.match(name):
        raise ValueError("raw_data_name 只能包含字母、数字、下划线，且以字母开头")
    biz = int(params["bk_biz_id"])
    scope = validate_scope(params.get("access_conf_info"))
    if meta_db.query_one("SELECT id FROM access_raw_data WHERE bk_biz_id=? AND raw_data_name=?", (biz, name)):
        raise ValueError(f"业务 {biz} 下已存在数据源 {name}")

    # 1. 建 Raw Topic，分区数决定下游清洗的最大并发
    topic = raw_topic_name(name, biz)
    kafka_admin.ensure_topic(settings.OUTER_KAFKA, topic, settings.TOPIC_PARTITIONS)

    # 2. 写 Raw Data，自增 id 就是 Data ID（原平台还会向 GSE 登记路由，Queue 场景只是元数据，这里省略）
    raw_data_id = meta_db.execute(
        "INSERT INTO access_raw_data (bk_biz_id, raw_data_name, raw_data_alias, data_scenario, "
        "data_encoding, topic, created_by) VALUES (?,?,?,?,?,?,?)",
        (biz, name, raw.get("raw_data_alias"), "queue", raw.get("data_encoding") or "UTF-8",
         topic, params.get("bk_username")),
    )

    # 3. 保存资源配置（原平台会加密 password，这里只在返回时脱敏）
    meta_db.execute("INSERT INTO access_resource_info (raw_data_id, resource) VALUES (?,?)",
                    (raw_data_id, json.dumps(scope, ensure_ascii=False)))

    # 4. create_after：创建并启动 Puller（start_task=false 时只建配置）
    if params.get("start_task", True):
        start_puller(raw_data_id)
    return {"raw_data_id": raw_data_id, "topic": topic}


def update_deploy_plan(raw_data_id, params):
    get_raw_data(raw_data_id)
    scope = validate_scope(params.get("access_conf_info"))
    old = get_scope(raw_data_id)
    # 文档 6.6：没传新密码或传的是脱敏值时，保留原来的密码
    if scope.get("use_sasl") and scope.get("password") in (None, "", MASK):
        scope["password"] = old.get("password")
    meta_db.execute("UPDATE access_resource_info SET resource=? WHERE raw_data_id=?",
                    (json.dumps(scope, ensure_ascii=False), raw_data_id))
    start_puller(raw_data_id)   # 重新下发，配置变化后 Worker 会重启任务
    return {"raw_data_id": raw_data_id}


def get_deploy_plan(raw_data_id):
    raw = get_raw_data(raw_data_id)
    scope = get_scope(raw_data_id)
    if scope.get("password"):
        scope["password"] = MASK
    raw["scope"] = scope
    raw["tasks"] = worker.list_tasks(f"raw_data_id-{raw_data_id}")
    return raw


def get_raw_data(raw_data_id):
    raw = meta_db.query_one("SELECT * FROM access_raw_data WHERE id=?", (int(raw_data_id),))
    if raw is None:
        raise LookupError(f"Data ID {raw_data_id} 不存在")
    return raw


def get_scope(raw_data_id):
    row = meta_db.query_one("SELECT resource FROM access_resource_info WHERE raw_data_id=?", (int(raw_data_id),))
    if row is None:
        raise LookupError(f"Data ID {raw_data_id} 没有资源配置")
    return json.loads(row["resource"])


# ------------------------- Puller 任务（QueuePuller）-------------------------

def build_puller_conf(raw, scope):
    """文档 7.6：Kafka Connect Puller 配置"""
    conf = {
        "connector.class": "KafkaSourceConnector",
        "data.id": raw["id"],
        "tasks.max": scope["tasks"],
        "source.bootstrap.servers": scope["master"],
        "source.topic": scope["topic"],
        "source.group.id": scope["group"],
        "source.auto.offset.reset": scope["auto_offset_reset"],
        "destination.topic": raw["topic"],
        "dest.kafka.bs": settings.OUTER_KAFKA,
        "tag": raw["raw_data_name"],
    }
    if scope.get("use_sasl"):
        conf.update({
            "source.security.protocol": scope["security_protocol"],
            "source.sasl.mechanism": scope["sasl_mechanism"],
            "source.sasl.jaas.user": scope["user"],
            "source.sasl.jaas.password": scope["password"],
        })
    return conf


def start_puller(raw_data_id):
    raw = get_raw_data(raw_data_id)
    scope = get_scope(raw_data_id)
    worker.start_task(
        puller_name(raw_data_id), build_puller_conf(raw, scope),
        processing_id=f"raw_data_id-{raw_data_id}", task_type="puller",
        source=f"queue#{raw_data_id}", sink=f"kafka#{raw['topic']}",
    )


def stop_puller(raw_data_id):
    worker.stop_task(puller_name(raw_data_id))
