"""和 Java 数据面 Worker 交互，并维护任务记录 databus_connector_task。

对应原平台 databus/task/kafka/task.py：通过 Kafka Connect REST 接口启停 connector。
"""
import logging
import time

import requests

import meta_db
import settings

log = logging.getLogger(__name__)


def start_task(name, conf, processing_id, task_type, source, sink):
    """创建或重启一个 connector（文档 18.7.5：已存在时 Worker 会先停旧任务再按新配置启动）。"""
    meta_db.execute(
        "INSERT OR REPLACE INTO databus_connector_task "
        "(connector_task_name, processing_id, task_type, source, sink, status) VALUES (?,?,?,?,?,?)",
        (name, processing_id, task_type, source, sink, "starting"),
    )
    conf = {k: str(v) for k, v in conf.items()}   # Kafka Connect 的配置值都是字符串

    last_error = None
    for _ in range(3):                             # 失败重试 3 次（文档 18.7.5）
        try:
            resp = requests.put(f"{settings.WORKER_URL}/connectors/{name}/config", json=conf, timeout=90)
            if resp.status_code == 400:            # 配置错误，重试没有意义
                raise ValueError(f"Worker 拒绝了配置：{resp.json().get('error')}")
            resp.raise_for_status()
            _set_status(name, "running")
            log.info("下发 connector %s 成功", name)
            return resp.json()
        except requests.RequestException as e:
            last_error = e
            time.sleep(1)
    _set_status(name, "failed")
    raise RuntimeError(f"下发 connector {name} 失败：{last_error}")


def stop_task(name):
    try:
        resp = requests.delete(f"{settings.WORKER_URL}/connectors/{name}", timeout=90)
        if resp.status_code not in (200, 404):
            resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"停止 connector {name} 失败：{e}")
    _set_status(name, "stopped")


def task_status(name):
    """查询 Worker 中的实时状态和指标；Worker 不可用时返回 None"""
    try:
        resp = requests.get(f"{settings.WORKER_URL}/connectors/{name}", timeout=5)
        return resp.json() if resp.status_code == 200 else None
    except requests.RequestException:
        return None


def list_tasks(processing_id=None):
    if processing_id:
        rows = meta_db.query_all("SELECT * FROM databus_connector_task WHERE processing_id=?", (processing_id,))
    else:
        rows = meta_db.query_all("SELECT * FROM databus_connector_task ORDER BY updated_at")
    for row in rows:
        info = task_status(row["connector_task_name"])
        row["worker"] = {"tasks": info["tasks"]} if info else None
    return rows


def _set_status(name, status):
    meta_db.execute(
        "UPDATE databus_connector_task SET status=?, updated_at=CURRENT_TIMESTAMP WHERE connector_task_name=?",
        (status, name),
    )
