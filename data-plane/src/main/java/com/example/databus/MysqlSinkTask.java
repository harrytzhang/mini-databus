package com.example.databus;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL 入库任务（对应文档 18.8 MysqlSinkTask / JdbcAbstractSinkTask / BatchRecords）。
 *
 *   inner Topic --poll--> Avro 解码（一条消息多行）-> 由 key 算 thedate -> 攒批
 *               -> JDBC executeBatch -> commit -> 提交 Kafka offset
 *
 * 高吞吐的关键：
 * - JDBC URL 带 rewriteBatchedStatements=true，驱动把一批 INSERT 合并成一条多值 INSERT（文档 18.7.3）
 * - 每 batch.size 行提交一次事务
 * - tasks.max 个任务并行，每个任务有自己的连接，负责不同分区
 *
 * 异常分类处理（文档 18.8.8 的简化版，按 MySQL 错误码判断，比按错误信息字符串判断更稳定）：
 * - 连接断开：重连后整批重写一次，还失败就抛异常，任务重启后从上次 offset 重放（不丢数据）
 * - 行级错误（主键冲突、超长、非法字符、没有对应分区等）：改为逐条写入，只丢弃出错的行
 * - 其他错误：抛异常，任务失败重启
 */
public class MysqlSinkTask extends BaseTask {
    private static final Set<Integer> ROW_ERRORS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            1048, // 列不能为 NULL
            1062, // 唯一键冲突
            1264, // 数值越界
            1265, // 数据被截断
            1292, // 值不合法
            1366, // 字符串编码不合法
            1406, // 数据过长
            1526  // 没有对应的分区
    )));
    private static final String UNIQUE_KEY_COLUMN = "____key";
    private static final List<String> SYSTEM_COLUMNS = Collections.unmodifiableList(
            Arrays.asList("thedate", "dtEventTime", "dtEventTimeStamp", "localTime"));

    private final String url;
    private final String user;
    private final String password;
    private final List<String> bizColumns;
    private final boolean uniqueKey;
    private final int[] keyIndexes;   // storage_keys 在 bizColumns 中的位置
    private final int batchSize;
    private final String insertSql;
    private final List<Object[]> buffer = new ArrayList<>();
    private Connection conn;
    private int maxDate;

    public MysqlSinkTask(String connectorName, int taskId, Map<String, String> conf) {
        super(connectorName, taskId, conf);
        this.url = get("connection.url");
        this.user = get("connection.user");
        this.password = get("connection.password", "");
        this.bizColumns = splitList(get("columns"));
        this.uniqueKey = Boolean.parseBoolean(get("has.unique.key", "false"));
        this.batchSize = Integer.parseInt(get("batch.size", "2000"));

        // 唯一键字段排序，保证 ____key 的计算结果稳定（文档 18.8.2）
        List<String> keys = splitList(get("storage.keys", "")).stream().sorted().collect(Collectors.toList());
        if (uniqueKey && keys.isEmpty()) {
            throw new IllegalArgumentException("唯一键模式需要配置 storage.keys");
        }
        this.keyIndexes = new int[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            keyIndexes[i] = bizColumns.indexOf(keys.get(i));
            if (keyIndexes[i] < 0) {
                throw new IllegalArgumentException("storage.keys 中的字段不在 columns 中：" + keys.get(i));
            }
        }
        this.insertSql = buildInsertSql(get("table.name"));
    }

    /** 普通模式 INSERT；唯一键模式 INSERT ... ON DUPLICATE KEY UPDATE（文档 18.8.3） */
    private String buildInsertSql(String table) {
        List<String> cols = new ArrayList<>(SYSTEM_COLUMNS);
        cols.addAll(bizColumns);
        if (uniqueKey) {
            cols.add(UNIQUE_KEY_COLUMN);
        }
        String quotedTable = Arrays.stream(table.split("\\.")).map(s -> "`" + s + "`").collect(Collectors.joining("."));
        String colSql = cols.stream().map(c -> "`" + c + "`").collect(Collectors.joining(","));
        String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(","));
        String sql = "INSERT INTO " + quotedTable + " (" + colSql + ") VALUES (" + placeholders + ")";
        if (uniqueKey) {
            sql += " ON DUPLICATE KEY UPDATE " + cols.stream()
                    .filter(c -> !c.equals(UNIQUE_KEY_COLUMN))
                    .map(c -> "`" + c + "`=VALUES(`" + c + "`)")
                    .collect(Collectors.joining(","));
        }
        return sql;
    }

    @Override
    protected Properties consumerConfig() {
        Properties p = new Properties();
        p.put("bootstrap.servers", get("source.bootstrap.servers"));
        p.put("group.id", "connect-" + connectorName);
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    @Override
    protected String sourceTopic() {
        return get("topics");
    }

    @Override
    protected void startTask() throws SQLException {
        conn = connect();
        maxDate = tomorrow();
        log.info("[{}] SQL：{}", name, insertSql);
    }

    @Override
    protected void process(ConsumerRecord<byte[], byte[]> record) throws SQLException {
        metrics.inc("in_msg");
        String key = record.key() == null ? "" : new String(record.key(), StandardCharsets.UTF_8);
        int theDate = parseTheDate(key);
        if (theDate < 0) {
            metrics.inc("bad_key");
            return;
        }
        if (theDate > maxDate) {    // 数据时间晚于明天，整条消息丢弃（文档 18.8.4）
            metrics.inc("drop_future_msg");
            return;
        }

        List<GenericRecord> rows;
        try {
            rows = AvroCodec.decode(record.value());
        } catch (IOException | AvroRuntimeException e) {
            log.warn("[{}] Avro 解码失败 partition={} offset={}：{}", name, record.partition(), record.offset(), e.toString());
            metrics.inc("bad_value");
            return;
        }
        for (GenericRecord rec : rows) {
            buffer.add(toRow(theDate, rec));
            if (buffer.size() >= batchSize) {
                writeBuffer();  // 攒够一批立即写（文档 18.8.7）
            }
        }
    }

    @Override
    protected void flush() throws SQLException {
        writeBuffer();
        maxDate = tomorrow();
    }

    @Override
    protected void stopTask() {
        closeQuietly();
    }

    // ------------------------- 行转换 -------------------------

    /** key 前 14 位是 yyyyMMddHHmmss，取前 8 位作为 thedate；不合法返回 -1（文档 18.8.5） */
    private static int parseTheDate(String key) {
        if (key.length() < 14 || !key.substring(0, 14).chars().allMatch(Character::isDigit)) {
            return -1;
        }
        int year = Integer.parseInt(key.substring(0, 4));
        return year < 2000 || year > 2100 ? -1 : Integer.parseInt(key.substring(0, 8));
    }

    private Object[] toRow(int theDate, GenericRecord rec) {
        Object[] row = new Object[SYSTEM_COLUMNS.size() + bizColumns.size() + (uniqueKey ? 1 : 0)];
        row[0] = theDate;
        row[1] = normalize(field(rec, "dtEventTime"));
        row[2] = field(rec, "dtEventTimeStamp");
        row[3] = normalize(field(rec, "localTime"));
        for (int i = 0; i < bizColumns.size(); i++) {
            row[4 + i] = normalize(field(rec, bizColumns.get(i)));
        }
        if (uniqueKey) {
            row[row.length - 1] = uniqueKey(row);
        }
        return row;
    }

    /** 消息里没有这一列（比如表加了字段，但旧消息还没有）时返回 null */
    private static Object field(GenericRecord rec, String name) {
        return rec.getSchema().getField(name) == null ? null : rec.get(name);
    }

    /** Avro 的 Utf8 转 String；NaN / Infinity 写不进 MySQL，置为 null */
    private static Object normalize(Object v) {
        if (v instanceof CharSequence) {
            return v.toString();
        }
        if (v instanceof Double && (((Double) v).isNaN() || ((Double) v).isInfinite())) {
            return null;
        }
        if (v instanceof Float && (((Float) v).isNaN() || ((Float) v).isInfinite())) {
            return null;
        }
        return v;
    }

    /** ____key = md5(keys 用 "-" 拼接 + "_" + keys 用 "+" 拼接)（文档 18.8.3） */
    private String uniqueKey(Object[] row) {
        List<String> values = new ArrayList<>();
        for (int idx : keyIndexes) {
            values.add(String.valueOf(row[SYSTEM_COLUMNS.size() + idx]));
        }
        String raw = String.join("-", values) + "_" + String.join("+", values);
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(raw.getBytes(StandardCharsets.UTF_8));
            return String.format("%032x", new BigInteger(1, digest));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // ------------------------- 写库 -------------------------

    private void writeBuffer() throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        write(buffer);
        metrics.inc("out", buffer.size());
        buffer.clear();
    }

    private void write(List<Object[]> rows) throws SQLException {
        try {
            executeBatch(rows);
        } catch (SQLException e) {
            rollbackQuietly();
            if (isConnectionError(e)) {
                log.warn("[{}] 连接异常（{}），重连后重写整批", name, e.getMessage());
                reconnect();
                executeBatch(rows);              // 再失败就抛出，任务重启
            } else if (ROW_ERRORS.contains(e.getErrorCode())) {
                log.warn("[{}] 批量写入出现行级错误（{}），改为逐条写入", name, e.getMessage());
                writeOneByOne(rows);
            } else {
                throw e;
            }
        }
    }

    private void executeBatch(List<Object[]> rows) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (Object[] row : rows) {
                bind(ps, row);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        conn.commit();
    }

    private void writeOneByOne(List<Object[]> rows) throws SQLException {
        int dropped = 0;
        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (Object[] row : rows) {
                try {
                    bind(ps, row);
                    ps.executeUpdate();
                } catch (SQLException e) {
                    if (!ROW_ERRORS.contains(e.getErrorCode())) {
                        throw e;
                    }
                    dropped++;
                }
            }
        }
        conn.commit();
        if (dropped > 0) {
            metrics.inc("drop", dropped);
            log.warn("[{}] 丢弃 {} 行无法写入的数据", name, dropped);
        }
    }

    private static void bind(PreparedStatement ps, Object[] row) throws SQLException {
        for (int i = 0; i < row.length; i++) {
            ps.setObject(i + 1, row[i]);
        }
    }

    private static boolean isConnectionError(SQLException e) {
        String state = e.getSQLState();
        return e instanceof SQLRecoverableException
                || e instanceof SQLNonTransientConnectionException
                || e instanceof SQLTransientConnectionException
                || (state != null && state.startsWith("08")); // SQLState 08xxx 表示连接类错误
    }

    private Connection connect() throws SQLException {
        Connection c = DriverManager.getConnection(url, user, password);
        c.setAutoCommit(false);
        return c;
    }

    private void reconnect() throws SQLException {
        closeQuietly();
        conn = connect();
    }

    private void rollbackQuietly() {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // 连接已断开时回滚会失败，忽略
        }
    }

    private void closeQuietly() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // 忽略
            }
        }
    }

    private static int tomorrow() {
        return Integer.parseInt(LocalDate.now().plusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE));
    }

    private static List<String> splitList(String s) {
        return Arrays.stream(s.split(",")).map(String::trim).filter(x -> !x.isEmpty()).collect(Collectors.toList());
    }
}
