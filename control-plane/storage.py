"""MySQL 入库（对应 data_storage_views + MysqlShipper，文档 18.5 / 18.7）。

添加入库的流程：
    校验参数 -> 登记存储（StoreKit）-> prepare 建表 -> 生成 MysqlSinkConnector 配置 -> 下发到 Worker
原平台第 4 步之后用 Celery 异步执行；这里同步执行，接口返回即表示任务已下发，更容易理解。
"""
import json

import access
import clean
import kafka_admin
import meta_db
import settings
import storekit
import worker


def mysql_name(rt_id):
    """文档 18.7.4：connector 名 = mysql-table_{rt_id}"""
    return f"mysql-table_{rt_id}"


def create_data_storage(params):
    if params.get("storage_type") != "mysql":
        raise ValueError("本项目只实现 storage_type=mysql")
    if params.get("data_type", "clean") != "clean":
        raise ValueError("本项目只支持 data_type=clean")
    cluster_name = params.get("storage_cluster") or "mysql-default"
    if cluster_name not in settings.STORAGE_CLUSTERS:
        raise ValueError(f"存储集群不存在：{cluster_name}")

    raw = access.get_raw_data(params["raw_data_id"])
    rt_id = clean.rt_id_of(raw["bk_biz_id"], params["result_table_name"])
    rt = clean.get_clean(rt_id)
    if meta_db.query_one("SELECT 1 FROM storage_result_table WHERE result_table_id=? AND storage_type='mysql'",
                         (rt_id,)):
        raise ValueError(f"{rt_id} 已经有 MySQL 存储")

    # fields 只用来声明索引字段和唯一键字段；字段本身以结果表为准
    rt_names = {f["field_name"] for f in storekit.business_fields(rt["fields"])}
    req_fields = params.get("fields") or []
    if req_fields and all(f.get("is_dimension") for f in req_fields):
        raise ValueError("字段不能全部是维度字段")   # 文档 18.5.1 的校验
    for f in req_fields:
        if f["field_name"] not in rt_names:
            raise ValueError(f"字段 {f['field_name']} 不在结果表中")
    has_unique_key = bool((params.get("config") or {}).get("has_unique_key"))
    storage_config = {
        "indexed_fields": [f["field_name"] for f in req_fields if f.get("is_index")],
        "storage_keys": sorted(f["field_name"] for f in req_fields if f.get("is_key")),
        "has_unique_key": has_unique_key,
    }
    if has_unique_key and not storage_config["storage_keys"]:
        raise ValueError("唯一键模式至少需要一个 is_key 字段")

    expires = int(params.get("expires") or 7)
    meta_db.execute(
        "INSERT INTO storage_result_table (result_table_id, storage_type, cluster_name, "
        "physical_table_name, expires, storage_config) VALUES (?,?,?,?,?,?)",
        (rt_id, "mysql", cluster_name, storekit.physical_table_name(raw["bk_biz_id"], rt["result_table_name"]),
         f"{expires}d", json.dumps(storage_config)),
    )
    start_mysql(rt_id)
    return {"result_table_id": rt_id, "physical_table_name": get_storage_info(rt_id)["physical_table_name"]}


def delete_data_storage(rt_id):
    info = get_storage_info(rt_id)
    stop_mysql(rt_id)
    meta_db.execute("DELETE FROM storage_result_table WHERE result_table_id=? AND storage_type='mysql'", (rt_id,))
    storekit.rename_to_delete(info)
    return {"result_table_id": rt_id}


def get_storage_info(rt_id):
    """组装 StoreKit 和 MysqlShipper 需要的全部信息"""
    row = meta_db.query_one("SELECT * FROM storage_result_table WHERE result_table_id=? AND storage_type='mysql'",
                            (rt_id,))
    if row is None:
        raise LookupError(f"{rt_id} 没有 MySQL 存储")
    return {
        "result_table_id": rt_id,
        "physical_table_name": row["physical_table_name"],
        "fields": clean.get_clean(rt_id)["fields"],
        "storage_config": json.loads(row["storage_config"]),
        "expires_days": int(row["expires"].rstrip("d")),
        "cluster": settings.STORAGE_CLUSTERS[row["cluster_name"]],
    }


def list_storage_infos():
    rows = meta_db.query_all("SELECT result_table_id FROM storage_result_table WHERE storage_type='mysql'")
    return [get_storage_info(r["result_table_id"]) for r in rows]


# ------------------------- 入库任务（MysqlShipper）-------------------------

def jdbc_url(cluster, db):
    """文档 18.7.3。rewriteBatchedStatements=true 是写入性能的关键参数"""
    host = cluster["host"]
    if ":" in host:   # IPv6 地址
        address = f"address=(protocol=tcp)(host={host})(port={cluster['port']})"
    else:
        address = f"{host}:{cluster['port']}"
    return (f"jdbc:mysql://{address}/{db}?autoReconnect=true&useServerPrepStmts=false"
            f"&rewriteBatchedStatements=true&useSSL=false&nullCatalogMeansCurrent=true"
            f"&characterEncoding=utf8&allowPublicKeyRetrieval=true")


def build_mysql_conf(info, tasks_max):
    """文档 18.7.4：MySQL connector 配置"""
    rt_id = info["result_table_id"]
    cluster = info["cluster"]
    db = info["physical_table_name"].split(".")[0]
    cfg = info["storage_config"]
    return {
        "connector.class": "MysqlSinkConnector",
        "rt.id": rt_id,
        "topics": clean.inner_topic(rt_id),
        "source.bootstrap.servers": settings.INNER_KAFKA,
        "tasks.max": tasks_max,
        "connection.url": jdbc_url(cluster, db),
        "connection.user": cluster["user"],
        "connection.password": cluster["password"],
        "table.name": info["physical_table_name"],
        "columns": ",".join(f["field_name"] for f in storekit.business_fields(info["fields"])),
        "batch.size": settings.MYSQL_BATCH_SIZE,
        "has.unique.key": str(cfg["has_unique_key"]).lower(),
        "storage.keys": ",".join(cfg["storage_keys"]),
    }


def start_mysql(rt_id):
    info = get_storage_info(rt_id)
    storekit.prepare(info)   # 文档 18.7.1 NeedPrepareShipper：启动前先建表 / 补字段
    topic = clean.inner_topic(rt_id)
    tasks_max = kafka_admin.partition_count(settings.INNER_KAFKA, topic)   # 并发度 = inner topic 分区数
    if tasks_max == 0:
        raise RuntimeError(f"inner topic {topic} 不存在")
    worker.start_task(
        mysql_name(rt_id), build_mysql_conf(info, tasks_max),
        processing_id=rt_id, task_type="mysql",
        source=f"kafka#{topic}", sink=f"mysql#{info['physical_table_name']}",
    )


def stop_mysql(rt_id):
    worker.stop_task(mysql_name(rt_id))
