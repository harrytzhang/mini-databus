package com.example.databus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Puller 拉取任务（对应文档第 7 章 KafkaSourceTask）。
 *
 *   外部 Kafka（source.topic）--poll--> PullerTask --send--> Raw Topic（destination.topic）
 *
 * - value 原样转发，不做解析；key 写成 ds={yyyyMMddHHmmss}&tag={tag}（文档 7.10）
 * - 外部分区 i 写到 Raw 分区 i % N，保证同一分区内的数据顺序不变
 * - 和原实现的区别：原 Kafka Puller 使用 auto commit，发送失败可能丢数据（文档 L4）。
 *   这里继承 BaseTask，Producer 全部发送成功后才提交外部 offset，是至少一次语义。
 */
public class PullerTask extends BaseTask {
    private static final DateTimeFormatter DS =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());

    private final String destServers;
    private final String destTopic;
    private final String tag;
    private final AtomicReference<Exception> sendError = new AtomicReference<>();
    private KafkaProducer<byte[], byte[]> producer;
    private int destPartitions;

    public PullerTask(String connectorName, int taskId, Map<String, String> conf) {
        super(connectorName, taskId, conf);
        this.destServers = get("dest.kafka.bs");
        this.destTopic = get("destination.topic");
        this.tag = get("tag", "");
        get("source.bootstrap.servers"); // 提前校验必填项
        get("source.topic");
        get("source.group.id");
    }

    @Override
    protected Properties consumerConfig() {
        Properties p = new Properties();
        p.put("bootstrap.servers", get("source.bootstrap.servers"));
        p.put("group.id", get("source.group.id"));                       // 外部集群上的消费组
        p.put("auto.offset.reset", get("source.auto.offset.reset", "latest"));

        // 外部 Kafka 开启 SASL 认证时（文档 5.2.2）
        String protocol = get("source.security.protocol", "");
        if (!protocol.isEmpty()) {
            String mechanism = get("source.sasl.mechanism", "PLAIN");
            String module = mechanism.startsWith("SCRAM")
                    ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                    : "org.apache.kafka.common.security.plain.PlainLoginModule";
            p.put("security.protocol", protocol);
            p.put("sasl.mechanism", mechanism);
            p.put("sasl.jaas.config", String.format("%s required username=\"%s\" password=\"%s\";",
                    module, get("source.sasl.jaas.user"), get("source.sasl.jaas.password")));
        }
        return p;
    }

    @Override
    protected String sourceTopic() {
        return get("source.topic");
    }

    @Override
    protected void startTask() {
        producer = Kafkas.producer(destServers);
        destPartitions = producer.partitionsFor(destTopic).size();
    }

    @Override
    protected void process(ConsumerRecord<byte[], byte[]> record) {
        metrics.inc("in");
        long ts = record.timestamp() > 0 ? record.timestamp() : System.currentTimeMillis();
        byte[] key = ("ds=" + DS.format(Instant.ofEpochMilli(ts)) + "&tag=" + tag).getBytes(StandardCharsets.UTF_8);
        int partition = record.partition() % destPartitions;
        producer.send(new ProducerRecord<>(destTopic, partition, key, record.value()),
                Kafkas.callback(sendError, metrics, "out"));
    }

    @Override
    protected void flush() {
        Kafkas.flush(producer, sendError);
    }

    @Override
    protected void stopTask() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
