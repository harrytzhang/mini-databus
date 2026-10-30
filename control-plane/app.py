"""控制面 HTTP 接口（对应 DataAPI 的 access / databus / storekit 路由，文档 5.1 / 18.5）。

启动：python app.py
返回格式统一为 {"result": true/false, "data": ..., "message": "..."}
"""
import logging
import threading
import time
from datetime import date, datetime

from flask import Flask, jsonify, request

import access
import clean
import meta_db
import settings
import storage
import storekit
import worker

app = Flask(__name__)
if hasattr(app, "json"):                 # Flask 2.2+
    app.json.ensure_ascii = False
else:                                    # Flask 2.0 / 2.1（Python 3.6 只能装到 2.0.x）
    app.config["JSON_AS_ASCII"] = False
log = logging.getLogger("app")


def ok(data=None):
    return jsonify({"result": True, "data": data, "message": "ok"})


def fail(message, code):
    return jsonify({"result": False, "data": None, "message": message}), code


@app.errorhandler(ValueError)        # 参数错误
def on_value_error(e):
    return fail(str(e), 400)


@app.errorhandler(LookupError)       # 资源不存在（KeyError 也属于 LookupError，表示缺少必填参数）
def on_lookup_error(e):
    message = f"缺少参数：{e}" if isinstance(e, KeyError) else str(e)
    return fail(message, 400 if isinstance(e, KeyError) else 404)


@app.errorhandler(RuntimeError)      # 外部系统（Kafka / Worker / MySQL）出错
def on_runtime_error(e):
    log.exception("接口执行失败")
    return fail(str(e), 500)


def body():
    return request.get_json(force=True, silent=False) or {}


# ------------------------- 接入 -------------------------

@app.post("/v3/access/deploy_plan/")
def create_deploy_plan():
    return ok(access.create_deploy_plan(body()))


@app.put("/v3/access/deploy_plan/<int:raw_data_id>/")
def update_deploy_plan(raw_data_id):
    return ok(access.update_deploy_plan(raw_data_id, body()))


@app.get("/v3/access/deploy_plan/<int:raw_data_id>/")
def get_deploy_plan(raw_data_id):
    return ok(access.get_deploy_plan(raw_data_id))


# ------------------------- 清洗 -------------------------

@app.post("/v3/databus/cleans/")
def create_clean():
    return ok(clean.create_clean(body()))


@app.put("/v3/databus/cleans/<rt_id>/")
def update_clean(rt_id):
    return ok(clean.update_clean(rt_id, body()))


@app.get("/v3/databus/cleans/<rt_id>/")
def get_clean(rt_id):
    return ok(clean.get_clean(rt_id))


@app.post("/v3/databus/scenarios/setup_clean/")
def setup_clean():
    return ok(clean.setup_clean(body()))


# ------------------------- 任务启停 -------------------------
# storages 里 "kafka" 表示清洗任务（输出到 kafka），"mysql" 表示入库任务（文档 5.4 / 18.5.3）
# consume_position 只对清洗任务生效：continue（默认，补齐停止期间的数据）| tail（跳过停止期间的数据）

@app.post("/v3/databus/tasks/")
def start_tasks():
    params = body()
    rt_id = params["result_table_id"]
    storages = params.get("storages") or ["kafka"]
    consume_position = clean.validate_consume_position(params.get("consume_position"))
    if "kafka" in storages:
        clean.start_clean(rt_id, consume_position)
    if "mysql" in storages:
        storage.start_mysql(rt_id)
    return ok({"result_table_id": rt_id, "started": storages, "consume_position": consume_position})


@app.delete("/v3/databus/tasks/<rt_id>/")
def stop_tasks(rt_id):
    storages = request.args.get("storages", "kafka,mysql").split(",")
    if "mysql" in storages:
        storage.stop_mysql(rt_id)
    if "kafka" in storages:
        clean.stop_clean(rt_id)
    return ok({"result_table_id": rt_id, "stopped": storages})


@app.get("/v3/databus/tasks/")
def list_tasks():
    return ok(worker.list_tasks(request.args.get("processing_id")))


# ------------------------- 入库 -------------------------

@app.post("/v3/databus/data_storages/")
def create_data_storage():
    return ok(storage.create_data_storage(body()))


@app.delete("/v3/databus/data_storages/<rt_id>/")
def delete_data_storage(rt_id):
    return ok(storage.delete_data_storage(rt_id))


# ------------------------- StoreKit -------------------------

@app.get("/v3/storekit/mysql/<rt_id>/prepare/")
def mysql_prepare(rt_id):
    return ok(storekit.prepare(storage.get_storage_info(rt_id)))


@app.get("/v3/storekit/mysql/<rt_id>/maintain/")
def mysql_maintain(rt_id):
    return ok(storekit.maintain(storage.get_storage_info(rt_id)))


@app.get("/v3/storekit/mysql/maintain_all/")
def mysql_maintain_all():
    return ok(maintain_all())


def maintain_all():
    result = {}
    for info in storage.list_storage_infos():
        try:
            result[info["result_table_id"]] = storekit.maintain(info)
        except Exception as e:   # 一张表失败不影响其他表
            log.exception("分区维护失败：%s", info["result_table_id"])
            result[info["result_table_id"]] = {"error": str(e)}
    return result


def maintain_loop():
    """每天 MAINTAIN_HOUR 点执行一次全量分区维护（原平台用 celery-beat 定时，每天 07:30）"""
    done_day = None
    while True:
        now = datetime.now()
        if now.hour == settings.MAINTAIN_HOUR and done_day != date.today():
            log.info("开始每日分区维护")
            maintain_all()
            done_day = date.today()
        time.sleep(60)


def status_sync_loop():
    """定时把 Worker 中任务的实际状态同步到 databus_connector_task.status（原平台由定时任务巡检 connector 状态）"""
    while True:
        time.sleep(settings.STATUS_SYNC_INTERVAL)
        try:
            worker.sync_all()
        except Exception:   # 一轮失败不影响下一轮
            log.exception("任务状态同步失败")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    meta_db.init()
    threading.Thread(target=maintain_loop, name="maintain", daemon=True).start()
    threading.Thread(target=status_sync_loop, name="status-sync", daemon=True).start()
    app.run(host="0.0.0.0", port=settings.API_PORT, threaded=True)
