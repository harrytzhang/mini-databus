package com.example.databus;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ETL 规则树引擎（对应文档第 10 章 databus-pipe-extractor 的简化版）。
 *
 * 清洗配置 etl.conf 示例：
 * <pre>
 * {
 *   "time_field": "event_time", "time_format": "yyyy-MM-dd HH:mm:ss", "timezone": 8,
 *   "rule": {"type": "json", "next": [ ... ]}
 * }
 * </pre>
 *
 * 每个节点处理“当前值”，再把结果交给子节点 next：
 *   json     把字符串解析成 JSON（Map / List）
 *   split    按分隔符切分字符串，得到 List，配置 delimiter
 *   regex    正则提取，配置 pattern 和 keys：第 i 个捕获组的值放到 keys[i]
 *   access   取 Map 的 key 或 List 的下标，配置 key
 *   iterate  遍历 List，每个元素各自执行子节点 -> 一条消息变多行（文档 10.6 一对多）
 *   assign   叶子节点：把值赋给结果表字段，配置 fields: [{key, as}]
 *
 * 同一节点下多个子节点的结果做笛卡尔积合并，例如：
 *   子节点 A 产出 [{order_id: 1}]，子节点 B（iterate）产出 [{item: a}, {item: b}]
 *   合并结果     [{order_id: 1, item: a}, {order_id: 1, item: b}]
 *
 * 最后按结果表字段做类型转换，并生成平台时间字段（文档 18.8.6）：
 *   dtEventTimeStamp 数据时间（毫秒）  dtEventTime 数据时间字符串
 *   localTime        处理时间字符串    timestamp   数据时间（秒）
 */
public class Etl {

    /** 单条数据清洗失败（坏数据）：任务跳过这条并上报，不会中断 */
    public static class EtlException extends Exception {
        public EtlException(String message) {
            super(message);
        }
    }

    /** 平台生成的字段，不从规则树取值 */
    static final Set<String> SYSTEM_FIELDS = Set.of("dtEventTimeStamp", "dtEventTime", "localTime", "timestamp");
    private static final DateTimeFormatter OUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Map<String, Object> rule;
    private final List<String[]> fields = new ArrayList<>(); // [字段名, 类型]
    private final String timeField;
    private final String timeFormat;
    private final DateTimeFormatter timeFormatter;
    private final ZoneId zone;
    private final Map<String, Pattern> patterns = new HashMap<>();
    private long localSecond = -1;
    private String localTimeStr;

    @SuppressWarnings("unchecked")
    public Etl(Map<String, Object> conf, List<Map<String, Object>> rtFields) {
        this.rule = (Map<String, Object>) conf.get("rule");
        if (rule == null) {
            throw new IllegalArgumentException("etl.conf 缺少 rule");
        }
        for (Map<String, Object> f : rtFields) {
            String fieldName = (String) f.get("field_name");
            if (!SYSTEM_FIELDS.contains(fieldName)) {
                fields.add(new String[]{fieldName, (String) f.get("field_type")});
            }
        }
        this.timeField = (String) conf.get("time_field");
        this.timeFormat = String.valueOf(conf.getOrDefault("time_format", "yyyy-MM-dd HH:mm:ss"));
        this.timeFormatter = timeFormat.startsWith("unix_") ? null : DateTimeFormatter.ofPattern(timeFormat);
        this.zone = ZoneOffset.ofHours(((Number) conf.getOrDefault("timezone", 8)).intValue());
    }

    /** 输入一条原始消息，返回结构化的行；失败抛 EtlException */
    public List<Map<String, Object>> handle(String text) throws EtlException {
        List<Map<String, Object>> rows = runNode(rule, text);
        List<Map<String, Object>> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            result.add(finish(row));
        }
        return result;
    }

    // ------------------------- 规则树执行 -------------------------

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runNode(Map<String, Object> node, Object value) throws EtlException {
        String type = (String) node.get("type");
        switch (type) {
            case "json":
                try {
                    value = Worker.JSON.readValue(String.valueOf(value), Object.class);
                } catch (JsonProcessingException e) {
                    throw new EtlException("JSON 解析失败：" + e.getOriginalMessage());
                }
                break;
            case "split":
                String delimiter = (String) node.get("delimiter");
                value = new ArrayList<Object>(Arrays.asList(String.valueOf(value).split(Pattern.quote(delimiter), -1)));
                break;
            case "regex":
                Pattern pattern = patterns.computeIfAbsent((String) node.get("pattern"), Pattern::compile);
                Matcher m = pattern.matcher(String.valueOf(value));
                if (!m.find()) {
                    throw new EtlException("正则不匹配");
                }
                List<String> keys = (List<String>) node.get("keys");
                Map<String, Object> groups = new HashMap<>();
                for (int i = 0; i < keys.size() && i < m.groupCount(); i++) {
                    groups.put(keys.get(i), m.group(i + 1));
                }
                value = groups;
                break;
            case "access":
                value = get(value, node.get("key"));
                if (value == null) {
                    throw new EtlException("取不到 key：" + node.get("key"));
                }
                break;
            case "iterate":
                if (!(value instanceof List)) {
                    throw new EtlException("iterate 节点的输入不是数组");
                }
                List<Map<String, Object>> rows = new ArrayList<>();
                for (Object item : (List<Object>) value) {
                    rows.addAll(runChildren(node, item));
                }
                return rows;
            case "assign":
                Map<String, Object> row = new HashMap<>();
                for (Map<String, Object> f : (List<Map<String, Object>>) node.get("fields")) {
                    row.put((String) f.get("as"), get(value, f.get("key")));
                }
                return Collections.singletonList(row);
            default:
                throw new IllegalArgumentException("未知的节点类型：" + type);
        }
        return runChildren(node, value);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runChildren(Map<String, Object> node, Object value) throws EtlException {
        List<Map<String, Object>> rows = Collections.singletonList(new HashMap<>());
        List<Map<String, Object>> children = (List<Map<String, Object>>) node.getOrDefault("next", List.of());
        for (Map<String, Object> child : children) {
            List<Map<String, Object>> childRows = runNode(child, value);
            List<Map<String, Object>> merged = new ArrayList<>(rows.size() * childRows.size());
            for (Map<String, Object> a : rows) {      // 笛卡尔积合并
                for (Map<String, Object> b : childRows) {
                    Map<String, Object> m = new HashMap<>(a);
                    m.putAll(b);
                    merged.add(m);
                }
            }
            rows = merged;
        }
        return rows;
    }

    /** Map 按 key 取值，List 按下标取值，取不到返回 null */
    private static Object get(Object value, Object key) {
        if (value instanceof Map) {
            return ((Map<?, ?>) value).get(String.valueOf(key));
        }
        if (value instanceof List && key instanceof Number) {
            List<?> list = (List<?>) value;
            int idx = ((Number) key).intValue();
            return idx >= 0 && idx < list.size() ? list.get(idx) : null;
        }
        return null;
    }

    // ------------------------- 类型转换与时间字段 -------------------------

    private Map<String, Object> finish(Map<String, Object> row) throws EtlException {
        Map<String, Object> out = new HashMap<>();
        for (String[] f : fields) {
            Object v = row.get(f[0]);
            out.put(f[0], v == null ? null : convert(f[0], f[1], v));
        }
        long ts = parseTime(row.get(timeField));
        out.put("dtEventTimeStamp", ts);
        out.put("dtEventTime", OUT_FORMAT.format(Instant.ofEpochMilli(ts).atZone(zone)));
        out.put("localTime", localTime());
        out.put("timestamp", ts / 1000);
        return out;
    }

    private static Object convert(String name, String type, Object v) throws EtlException {
        try {
            switch (type) {
                case "int":
                    return v instanceof Number ? ((Number) v).intValue() : Integer.parseInt(v.toString().trim());
                case "long":
                    return v instanceof Number ? ((Number) v).longValue() : Long.parseLong(v.toString().trim());
                case "float":
                    return v instanceof Number ? ((Number) v).floatValue() : Float.parseFloat(v.toString().trim());
                case "double":
                    return v instanceof Number ? ((Number) v).doubleValue() : Double.parseDouble(v.toString().trim());
                case "boolean":
                    if (v instanceof Boolean) {
                        return v;
                    }
                    if (v instanceof Number) {
                        return ((Number) v).intValue() != 0;
                    }
                    String s = v.toString().trim().toLowerCase();
                    return "1".equals(s) || "true".equals(s) || "yes".equals(s);
                default: // string / text：对象和数组转成 JSON 字符串
                    return (v instanceof Map || v instanceof List) ? Worker.JSON.writeValueAsString(v) : v.toString();
            }
        } catch (NumberFormatException | JsonProcessingException e) {
            throw new EtlException("字段 " + name + " 类型转换失败：" + v);
        }
    }

    private long parseTime(Object v) throws EtlException {
        if (v == null) {
            throw new EtlException("缺少时间字段 " + timeField);
        }
        try {
            switch (timeFormat) {
                case "unix_s":
                    return (long) (Double.parseDouble(v.toString()) * 1000);
                case "unix_ms":
                    return (long) Double.parseDouble(v.toString());
                default:
                    return LocalDateTime.parse(v.toString(), timeFormatter).atZone(zone).toInstant().toEpochMilli();
            }
        } catch (DateTimeParseException | NumberFormatException e) {
            throw new EtlException("时间解析失败：" + v);
        }
    }

    /** 处理时间按秒缓存，避免每行都格式化一次 */
    private String localTime() {
        long now = System.currentTimeMillis() / 1000;
        if (now != localSecond) {
            localSecond = now;
            localTimeStr = OUT_FORMAT.format(Instant.ofEpochSecond(now).atZone(zone));
        }
        return localTimeStr;
    }
}
