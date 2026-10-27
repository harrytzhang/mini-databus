package com.example.databus;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Kafka Producer 工具。高吞吐相关的参数都集中在这里：
 * - linger.ms + batch.size：攒批发送，一次网络请求带很多消息
 * - compression.type=lz4：压缩，节省带宽
 * - acks=all + 幂等：可靠性优先，Producer 内部重试不会产生重复
 * - buffer.memory 满了 send() 会阻塞等待（背压），不会丢消息也不会撑爆内存
 *
 * Producer 是异步发送的，失败只能在回调里知道。这里把第一个失败记下来，
 * flush 时检查：有失败就抛异常，基类因此不会提交 offset（文档 9.9 / 12.2）。
 */
public final class Kafkas {
    private Kafkas() {
    }

    public static KafkaProducer<byte[], byte[]> producer(String servers) {
        Properties p = new Properties();
        p.put("bootstrap.servers", servers);
        p.put("key.serializer", ByteArraySerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        p.put("acks", "all");
        p.put("enable.idempotence", "true");
        p.put("linger.ms", "20");
        p.put("batch.size", String.valueOf(256 * 1024));
        p.put("compression.type", "lz4");
        p.put("buffer.memory", String.valueOf(128L * 1024 * 1024));
        return new KafkaProducer<>(p);
    }

    /** 发送回调：成功时计数，失败时记录异常 */
    public static Callback callback(AtomicReference<Exception> error, Metrics metrics, String counter) {
        return (metadata, exception) -> {
            if (exception != null) {
                error.compareAndSet(null, exception);
            } else {
                metrics.inc(counter);
            }
        };
    }

    /** 等待所有消息发送完成，有失败则抛异常 */
    public static void flush(KafkaProducer<byte[], byte[]> producer, AtomicReference<Exception> error) {
        producer.flush();
        Exception e = error.getAndSet(null);
        if (e != null) {
            throw new IllegalStateException("有消息发送失败", e);
        }
    }
}
