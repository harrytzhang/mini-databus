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
                _set_status(name, "failed")
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
    return _fetch(name)[1]


def _fetch(name):
    """返回 (Worker 是否正常应答, connector 信息)。

    (True, info)   connector 存在
    (True, None)   Worker 正常，但没有这个 connector（404）
    (False, None)  Worker 不可达、超时或出错：这时什么都判断不了
    """
    try:
        resp = requests.get(f"{settings.WORKER_URL}/connectors/{name}", timeout=5)
    except requests.RequestException:
        return False, None
    if resp.status_code == 200:
        return True, resp.json()
    if resp.status_code == 404:
        return True, None
    return False, None


def list_tasks(processing_id=None):
    if processing_id:
        rows = meta_db.query_all("SELECT * FROM databus_connector_task WHERE processing_id=?", (processing_id,))
    else:
        rows = meta_db.query_all("SELECT * FROM databus_connector_task ORDER BY updated_at")
    for row in rows:
        reachable, info = _fetch(row["connector_task_name"])
        _sync_row(row, reachable, info)    # 查询时顺便同步，返回的 status 就是最新的
        row["worker"] = {"tasks": info["tasks"]} if info else None
    return rows


# ------------------------- 状态同步 -------------------------
# databus_connector_task.status 原来只在下发 / 停止时写入，只代表“命令是否送达”。
# Worker 中的任务之后失败、恢复，记录都不会变，于是出现“记录是 running，实际是 FAILED”。
# 这里用 Worker 的实际状态去修正记录：查询时同步一次，后台再定时同步（app.py 中的 status_sync_loop）。
#
# 只同步这三种状态；starting（正在下发）和 stopped（用户停的）由 start_task / stop_task 负责，不去碰
SYNCABLE = ("running", "failed", "lost")


def observed_status(row_status, info):
    """根据 Worker 返回的信息推导任务的实际状态；判断不了时返回 None，表示保持原状态。

    lost     Worker 正常，但没有这个 connector（比如 Worker 换了机器、connectors.json 丢了）
    failed   有任务处于 FAILED，或者最近 TASK_FAILED_HOLD 秒内失败过
    running  所有任务都是 RUNNING，且最近没有失败
    """
    if info is None:
        # 只有“原来在跑、现在没了”才算 lost；下发时就失败（Worker 拒绝配置）的任务本来就不在 Worker 里，保持 failed
        return "lost" if row_status == "running" else None
    tasks = info.get("tasks") or []
    hold_ms = settings.TASK_FAILED_HOLD * 1000
    for t in tasks:
        since = t.get("ms_since_last_failure")   # 旧版 Worker 没有这个字段，按没失败过处理
        if t["state"].startswith("FAILED") or (since is not None and since < hold_ms):
            return "failed"
    if tasks and all(t["state"] == "RUNNING" for t in tasks):
        return "running"
    return None   # UNASSIGNED / STOPPED 是启动、停止过程中的过渡状态，不据此改记录


def _sync_row(row, reachable, info):
    """用 Worker 的实际状态修正一条任务记录，row["status"] 会随之更新"""
    old = row["status"]
    if not reachable or old not in SYNCABLE:
        return
    new = observed_status(old, info)
    if new is None or new == old:
        return
    # 带上旧状态做条件更新：读 Worker 期间如果有人启停了任务（状态已变成 starting / stopped），这次就不覆盖
    changed = meta_db.update(
        "UPDATE databus_connector_task SET status=?, updated_at=CURRENT_TIMESTAMP "
        "WHERE connector_task_name=? AND status=?",
        (new, row["connector_task_name"], old),
    )
    if not changed:
        return
    row["status"] = new
    reason = next((t["state"] for t in (info or {}).get("tasks", []) if t["state"].startswith("FAILED")), "")
    log.warning("任务 %s 状态 %s -> %s %s", row["connector_task_name"], old, new, reason)


def sync_all():
    """把所有需要同步的任务记录对齐到 Worker 的实际状态；Worker 不可达时什么都不改"""
    placeholders = ",".join("?" * len(SYNCABLE))
    rows = meta_db.query_all(
        f"SELECT * FROM databus_connector_task WHERE status IN ({placeholders})", SYNCABLE)
    for row in rows:
        reachable, info = _fetch(row["connector_task_name"])
        _sync_row(row, reachable, info)


def _set_status(name, status):
    meta_db.execute(
        "UPDATE databus_connector_task SET status=?, updated_at=CURRENT_TIMESTAMP WHERE connector_task_name=?",
        (status, name),
    )
