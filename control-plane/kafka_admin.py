"""Kafka topic 管理：建 topic、查分区数、扩分区（原平台用 kafka-python 做同样的事）。"""
import logging

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
