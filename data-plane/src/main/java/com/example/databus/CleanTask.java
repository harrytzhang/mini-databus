package com.example.databus;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * 清洗任务（对应文档第 9 章 TransformSinkTask / TransformSinkTaskKafka）。
 *
 *   Raw Topic --poll--> 解压 / 解码 -> ETL 规则树 -> 按 (源分区, 小时) 分组攒批
 *             -> Avro + Snappy 编码 --send--> inner Topic（table_{rt_id}）
 *
 * 要点：
 * 1. 坏数据不阻塞任务：计数，写到坏数据 topic，然后跳过（文档第 14 章）
 * 2. 一条输出消息里的行来自同一个源分区、同一个小时（文档 11.1），下游据此确定 thedate
 * 3. 输出写到与源相同编号的分区，所以 inner topic 分区数不能小于 raw topic（文档 9.2）
 * 4. 消息 key = {yyyyMMddHH}0000|{源分区}|{源 offset}：前 14 位是时间，后面是源 offset（文档 11.2）
 */
public class CleanTask extends BaseTask {
    private static final TypeReference<List<Map<String, Object>>> LIST_TYPE =
            new TypeReference<List<Map<String, Object>>>() { };
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() { };

    /** 一个缓冲组：同一源分区、同一小时的行 */
    private static class Buffer {
        final int partition;
        final String hour;
        final List<GenericRecord> rows = new ArrayList<>();
        long lastOffset;

        Buffer(int partition, String hour) {
            this.partition = partition;
            this.hour = hour;
        }
    }

    private final String rtId;
    private final String innerTopic;
    private final String badTopic;
    private final int recordsInMsg;
    private final Charset charset;
    private final Etl etl;
    private final Schema schema;
    private final CodecFactory codec;
    private final Map<String, Buffer> buffers = new HashMap<>();
    private final AtomicReference<Exception> sendError = new AtomicReference<>();
    private KafkaProducer<byte[], byte[]> producer;
    private int innerPartitions;

    public CleanTask(String connectorName, int taskId, Map<String, String> conf) {
        super(connectorName, taskId, conf);
        this.rtId = get("rt.id");
        this.innerTopic = get("sink.topic");
        this.badTopic = get("bad.data.topic", "");
        this.recordsInMsg = Integer.parseInt(get("producer.records.in.msg", "100"));
        this.charset = Charset.forName(get("data.encoding", "UTF-8"));
        this.codec = CodecFactory.fromString(get("avro.codec", "snappy"));
        List<Map<String, Object>> fields = getJson("rt.fields", LIST_TYPE);
        this.etl = new Etl(getJson("etl.conf", MAP_TYPE), fields);
        this.schema = AvroCodec.buildSchema(rtId, fields);
    }

    @Override
    protected Properties consumerConfig() {
        Properties p = new Properties();
        p.put("bootstrap.servers", get("source.bootstrap.servers"));
        p.put("group.id", "connect-" + connectorName);   // 和 Kafka Connect 一样：消费组 = connect-{connector 名}
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    @Override
    protected String sourceTopic() {
        return get("topics");
    }

    @Override
    protected void startTask() {
        producer = Kafkas.producer(get("producer.bootstrap.servers"));
        innerPartitions = producer.partitionsFor(innerTopic).size();
    }

    @Override
    protected void process(ConsumerRecord<byte[], byte[]> record) throws Exception {
        metrics.inc("in");
        byte[] raw = record.value() == null ? new byte[0] : record.value();
        List<Map<String, Object>> rows;
        try {
            if (raw.length > 1 && (raw[0] & 0xff) == 0x1f && (raw[1] & 0xff) == 0x8b) {
                raw = gunzip(raw); // gzip 魔数，先解压（文档 10.5）
            }
            String text = charset.newDecoder().decode(ByteBuffer.wrap(raw)).toString(); // 编码不合法会抛异常
            rows = etl.handle(text);
        } catch (Etl.EtlException | IOException e) {
            reportBad(record, raw, e);
            return;
        }

        int partition = record.partition();
        if (partition >= innerPartitions) {
            throw new IllegalStateException("inner topic 分区数 " + innerPartitions + " 小于源分区 " + partition);
        }
        for (Map<String, Object> row : rows) {
            String t = (String) row.get("dtEventTime");  // yyyy-MM-dd HH:mm:ss -> yyyyMMddHH
            String hour = t.substring(0, 4) + t.substring(5, 7) + t.substring(8, 10) + t.substring(11, 13);
            String key = partition + "|" + hour;
            Buffer buf = buffers.computeIfAbsent(key, k -> new Buffer(partition, hour));
            buf.rows.add(AvroCodec.toRecord(schema, row));
            buf.lastOffset = record.offset();
            if (buf.rows.size() >= recordsInMsg) {
                send(key);
            }
        }
        metrics.inc("rows", rows.size());
    }

    @Override
    protected void flush() throws IOException {
        for (String key : new ArrayList<>(buffers.keySet())) {
            send(key);
        }
        Kafkas.flush(producer, sendError);
    }

    @Override
    protected void stopTask() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(10));
        }
    }

    private void send(String key) throws IOException {
        Buffer buf = buffers.remove(key);
        byte[] value = AvroCodec.encode(schema, buf.rows, codec);
        byte[] msgKey = (buf.hour + "0000|" + buf.partition + "|" + buf.lastOffset).getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(innerTopic, buf.partition, msgKey, value),
                Kafkas.callback(sendError, metrics, "out_msg"));
    }

    private void reportBad(ConsumerRecord<byte[], byte[]> record, byte[] raw, Exception error) throws IOException {
        metrics.inc("bad");
        if (badTopic.isEmpty()) {
            return;
        }
        Map<String, Object> bad = new LinkedHashMap<>();
        bad.put("rt_id", rtId);
        bad.put("partition", record.partition());
        bad.put("offset", record.offset());
        bad.put("error", error.getMessage());
        bad.put("raw", new String(raw, 0, Math.min(raw.length, 1024), StandardCharsets.UTF_8));
        // 坏数据只用于排查，发送失败不影响主流程，所以不检查回调
        producer.send(new ProducerRecord<>(badTopic, Worker.JSON.writeValueAsBytes(bad)));
    }

    private static byte[] gunzip(byte[] data) throws IOException {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(data))) {
            return in.readAllBytes();
        }
    }
}
