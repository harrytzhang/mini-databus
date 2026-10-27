package com.example.databus;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * 任务基类：模板方法模式（对应文档 9.7 BkSinkTask）。
 *
 * Puller、Clean、MySQL 入库三类任务的主循环完全一样：
 *
 *   循环：
 *     poll 一批消息 -> 逐条 process()（发往下游 / 放进缓冲）
 *     攒够数量或到达时间间隔时：
 *        flush()          把缓冲全部写到下游，失败抛异常
 *        commitSync()     下游写成功后才提交 offset
 *
 * “先 flush 成功，再提交 offset”（文档 12.2 flush 阻断）保证了至少一次：
 * 任务崩溃时最多重复处理最后一批，不会丢数据。
 * 子类只需要实现下面几个抽象方法。
 */
public abstract class BaseTask {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final String connectorName;
    protected final String name;
    protected final Map<String, String> conf;
    protected final Metrics metrics;

    private final int maxPending;          // 最多处理多少条消息就强制 flush 一次
    private final long flushIntervalMs;    // 最长多久 flush 一次
    private volatile boolean stopped = false;
    private boolean failed = false;
    private int pending = 0;               // 已处理但还没提交 offset 的消息数
    private KafkaConsumer<byte[], byte[]> consumer;

    protected BaseTask(String connectorName, int taskId, Map<String, String> conf) {
        this.connectorName = connectorName;
        this.name = connectorName + "-" + taskId;
        this.conf = conf;
        this.metrics = new Metrics(name);
        this.maxPending = Integer.parseInt(get("max.pending.msgs", "20000"));
        this.flushIntervalMs = Long.parseLong(get("flush.interval.ms", "1000"));
    }

    // ------------------------- 子类实现 -------------------------

    /** 消费源 topic 的配置：至少包含 bootstrap.servers、group.id */
    protected abstract Properties consumerConfig();

    /** 源 topic */
    protected abstract String sourceTopic();

    /** 初始化下游资源（Producer、数据库连接等） */
    protected abstract void startTask() throws Exception;

    /** 处理一条消息 */
    protected abstract void process(ConsumerRecord<byte[], byte[]> record) throws Exception;

    /** 把缓冲的数据全部写到下游，失败必须抛异常 */
    protected abstract void flush() throws Exception;

    /** 释放资源 */
    protected void stopTask() {
    }

    // ------------------------- 固定流程 -------------------------

    public final void execute() throws Exception {
        try {
            startTask();
            Properties props = consumerConfig();
            props.put("key.deserializer", ByteArrayDeserializer.class.getName());
            props.put("value.deserializer", ByteArrayDeserializer.class.getName());
            props.put("enable.auto.commit", "false");      // 关键：offset 由我们在写成功后手动提交
            props.putIfAbsent("max.poll.records", "2000");  // 一次最多拉 2000 条
            props.putIfAbsent("fetch.min.bytes", "65536");  // 至少攒 64KB 再返回，减少请求次数
            props.putIfAbsent("fetch.max.wait.ms", "100");  // 但最多等 100ms，控制延迟
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(sourceTopic()), new RebalanceListener());
            log.info("[{}] 启动，消费 {}，消费组 {}", name, sourceTopic(), props.get("group.id"));

            long lastFlush = System.currentTimeMillis();
            while (!stopped) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    process(record);
                    pending++;
                }
                long now = System.currentTimeMillis();
                if (pending > 0 && (pending >= maxPending || now - lastFlush >= flushIntervalMs)) {
                    flushAndCommit();
                    lastFlush = now;
                }
                metrics.maybeReport();
            }
            flushAndCommit(); // 正常停止前最后提交一次
            log.info("[{}] 已停止", name);
        } catch (Exception e) {
            failed = true;    // 失败后不能再提交 offset，交给重启后重新消费
            throw e;
        } finally {
            if (consumer != null) {
                consumer.close(Duration.ofSeconds(10));
            }
            stopTask();
        }
    }

    public void stop() {
        stopped = true;
    }

    public String name() {
        return name;
    }

    public Metrics metrics() {
        return metrics;
    }

    private void flushAndCommit() throws Exception {
        if (pending == 0) {
            return;
        }
        flush();
        consumer.commitSync(); // 提交本轮已 poll 的位置
        pending = 0;
    }

    /** 分区被收回（扩缩容 / 其他任务加入）前，先把已处理的数据写完并提交，减少重复 */
    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (failed) {
                return;
            }
            try {
                flushAndCommit();
            } catch (Exception e) {
                throw new IllegalStateException("rebalance 前 flush 失败", e);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("[{}] 分配到分区 {}", name, partitions);
        }
    }

    // ------------------------- 配置读取工具 -------------------------

    /** 读取必填配置，缺失时报错 */
    protected String get(String key) {
        String value = conf.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("缺少配置项：" + key);
        }
        return value;
    }

    protected String get(String key, String defaultValue) {
        String value = conf.get(key);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    /** 读取 JSON 格式的配置项 */
    protected <T> T getJson(String key, TypeReference<T> type) {
        try {
            return Worker.JSON.readValue(get(key), type);
        } catch (IOException e) {
            throw new IllegalArgumentException("配置项 " + key + " 不是合法的 JSON", e);
        }
    }
}
