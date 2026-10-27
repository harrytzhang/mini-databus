package com.example.databus;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Avro 编解码（对应文档第 11 章：Avro Object Container + Snappy，一条消息包含多行）。
 *
 * 为什么一条消息打包多行？
 * Kafka 的开销主要按消息条数算（请求、索引、回调），100 行打成 1 条消息，消息数少 100 倍；
 * Avro 是带 Schema 的二进制格式，再加 Snappy 压缩，体积比 JSON 小很多。
 */
public final class AvroCodec {
    /** 清洗阶段平台追加的字段及类型 */
    private static final Map<String, Schema.Type> SYSTEM_FIELDS = new LinkedHashMap<>();

    static {
        SYSTEM_FIELDS.put("dtEventTimeStamp", Schema.Type.LONG);
        SYSTEM_FIELDS.put("dtEventTime", Schema.Type.STRING);
        SYSTEM_FIELDS.put("localTime", Schema.Type.STRING);
        SYSTEM_FIELDS.put("timestamp", Schema.Type.LONG);
    }

    private AvroCodec() {
    }

    /** 根据结果表字段生成 Schema，所有字段都允许为 null */
    public static Schema buildSchema(String rtId, List<Map<String, Object>> rtFields) {
        List<Schema.Field> fields = new ArrayList<>();
        for (Map<String, Object> f : rtFields) {
            String name = (String) f.get("field_name");
            if (!SYSTEM_FIELDS.containsKey(name)) {
                fields.add(nullable(name, avroType((String) f.get("field_type"))));
            }
        }
        SYSTEM_FIELDS.forEach((name, type) -> fields.add(nullable(name, type)));
        return Schema.createRecord("rt_" + rtId, null, "databus", false, fields);
    }

    private static Schema.Field nullable(String name, Schema.Type type) {
        Schema union = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type));
        return new Schema.Field(name, union, null, Schema.Field.NULL_DEFAULT_VALUE);
    }

    private static Schema.Type avroType(String fieldType) {
        switch (fieldType) {
            case "int":
                return Schema.Type.INT;
            case "long":
                return Schema.Type.LONG;
            case "float":
                return Schema.Type.FLOAT;
            case "double":
                return Schema.Type.DOUBLE;
            case "boolean":
                return Schema.Type.BOOLEAN;
            default:
                return Schema.Type.STRING;
        }
    }

    public static GenericRecord toRecord(Schema schema, Map<String, Object> row) {
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field f : schema.getFields()) {
            record.put(f.name(), row.get(f.name()));
        }
        return record;
    }

    public static byte[] encode(Schema schema, List<GenericRecord> rows, CodecFactory codec) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.setCodec(codec);
            writer.create(schema, out);
            for (GenericRecord row : rows) {
                writer.append(row);
            }
        }
        return out.toByteArray();
    }

    /** 解码时使用消息内自带的 Schema */
    public static List<GenericRecord> decode(byte[] data) throws IOException {
        List<GenericRecord> rows = new ArrayList<>();
        try (DataFileStream<GenericRecord> reader =
                     new DataFileStream<>(new ByteArrayInputStream(data), new GenericDatumReader<>())) {
            while (reader.hasNext()) {
                rows.add(reader.next());
            }
        }
        return rows;
    }
}
