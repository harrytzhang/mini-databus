#!/usr/bin/env bash
# 全部重置：停掉 Worker / API，删除本地运行状态，删除 Kafka、MySQL 容器及数据。
# 用法：在任意目录执行 ./reset.sh（脚本会自动切到项目根目录）
set -euo pipefail
cd "$(dirname "$0")"

PORTS="8083 8000"   # Worker、API

# 输出监听指定端口的进程 pid（没有则为空）
pids_on_port() {
  ss -lntp 2>/dev/null | awk -v p=":$1" '$4 ~ p"$"' | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u || true
}

echo "==> 1. 停掉 Worker 和 API（端口 ${PORTS}）"
for port in $PORTS; do
  pids=$(pids_on_port "$port")
  if [ -z "$pids" ]; then
    echo "    端口 $port 空闲"
    continue
  fi
  echo "    端口 $port 被进程 $(echo $pids) 占用，正在结束"
  kill $pids 2>/dev/null || true
  for _ in 1 2 3 4 5 6 7 8 9 10; do        # 最多等 10 秒正常退出
    [ -z "$(pids_on_port "$port")" ] && break
    sleep 1
  done
  pids=$(pids_on_port "$port")
  if [ -n "$pids" ]; then
    echo "    进程未退出，强制结束 $(echo $pids)"
    kill -9 $pids 2>/dev/null || true
    sleep 1
  fi
done

left=$(ss -lntp 2>/dev/null | grep -E ':(8083|8000)\s' || true)
if [ -n "$left" ]; then
  echo "    端口仍被占用，请手动处理：" >&2
  echo "$left" >&2
  exit 1
fi
echo "    端口已释放"

echo "==> 2. 删除本地运行状态"
rm -fv control-plane/meta.db data-plane/connectors.json

echo "==> 3. 删除 Kafka、MySQL 容器及数据"
docker compose down -v

echo
echo "重置完成。接下来："
echo "  docker compose up -d && sleep 20 && docker compose exec mysql mysqladmin ping -uroot -proot123"
echo "  终端 A：cd data-plane && java -jar target/mini-databus.jar"
echo "  终端 B：cd control-plane && source .venv/bin/activate && python app.py"
echo "  终端 C：cd control-plane && source .venv/bin/activate && python demo.py 100000"
