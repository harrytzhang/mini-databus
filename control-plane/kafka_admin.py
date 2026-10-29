"""Kafka topic 管理：建 topic、查分区数、扩分区、移动消费组位点（原平台用 kafka-python 做同样的事）。"""
import logging
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

import settings

log = logging.getLogger(__name__)


def _admin(servers):
    return AdminClient({"bootstrap.servers": servers})


def partition_count(servers, topic):
    """topic 不存在时返回 0。

    注意：不按 topic 名单独查询元数据，否则 broker 开启自动建 topic 时会顺手把它建出来。
    """
    topics = _admin(servers).list_topics(timeout=10).topics
    meta = topics.get(topic)
    if meta is None or meta.error is not None:
        return 0
    return len(meta.partitions)


def ensure_topic(servers, topic, partitions):
    """保证 topic 存在且分区数不少于 partitions，返回最终分区数。"""
    admin = _admin(servers)
    current = partition_count(servers, topic)
    if current == 0:
        new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=settings.TOPIC_REPLICATION)
        admin.create_topics([new_topic])[topic].result()
        log.info("创建 topic %s，分区数 %d", topic, partitions)
        return partitions
    if current < partitions:
        admin.create_partitions([NewPartitions(topic, partitions)])[topic].result()
        log.info("topic %s 分区从 %d 扩到 %d", topic, current, partitions)
        return partitions
    return current


def seek_group_to_end(servers, group_id, topic, partitions, retries=10, interval=3):
    """把消费组在 topic 每个分区上的已提交 offset 设为当前末尾（LEO），返回 {分区: offset}。

    只是提交 offset，不加入消费组。消费组里还有活跃成员时 broker 会拒绝提交
    （UNKNOWN_MEMBER_ID / ILLEGAL_GENERATION 等），所以调用前必须先停掉任务；
    任务刚停止、成员还没完全离组时会重试几次。
    """
    consumer = Consumer({"bootstrap.servers": servers, "group.id": group_id, "enable.auto.commit": False})
    try:
        offsets = []
        for p in range(partitions):
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            offsets.append(TopicPartition(topic, p, high))

        last_error = None
        for _ in range(retries):
            try:
                result = consumer.commit(offsets=offsets, asynchronous=False)
                errors = [tp for tp in result if tp.error is not None]
                if not errors:
                    log.info("消费组 %s 在 %s 上的位点已移到末尾：%s", group_id, topic,
                             {tp.partition: tp.offset for tp in offsets})
                    return {tp.partition: tp.offset for tp in offsets}
                last_error = errors[0].error
            except KafkaException as e:
                last_error = e
            log.warning("移动消费组 %s 位点失败（可能仍有活跃成员），%d 秒后重试：%s", group_id, interval, last_error)
            time.sleep(interval)
        raise RuntimeError(f"移动消费组 {group_id} 位点失败，请确认任务已停止：{last_error}")
    finally:
        consumer.close()
