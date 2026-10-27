package com.example.databus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * 一个 connector = 同一份配置 + tasks.max 个并行任务（每个任务一个线程）。
 *
 * 对应关系（文档 7.8 / 9.6 / 18.8.1）：
 *   connector.class = KafkaSourceConnector    -> PullerTask      外部 Kafka -> Raw Topic
 *   connector.class = TransformSinkConnector  -> CleanTask       Raw Topic -> 清洗 -> inner Topic
 *   connector.class = MysqlSinkConnector      -> MysqlSinkTask   inner Topic -> MySQL
 *
 * 同一 connector 的所有任务使用同一个消费组，Kafka 会把分区平均分给它们，
 * 所以 tasks.max 一般等于源 topic 的分区数，超过分区数的任务会空闲。
 * 任务抛异常时会在 5 秒后重建并从上次提交的 offset 继续（相当于 Connect 的任务重启）。
 */
public class Connector {
    private static final Logger log = LoggerFactory.getLogger(Connector.class);
    private static final long RESTART_BACKOFF_MS = 5000;

    private final String name;
    private final Map<String, String> conf;
    private final int tasksMax;
    private final AtomicReferenceArray<BaseTask> tasks;
    private final AtomicReferenceArray<String> states;
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean stopped = false;

    public Connector(String name, Map<String, String> conf) {
        this.name = name;
        this.conf = Collections.unmodifiableMap(new LinkedHashMap<>(conf));
        this.tasksMax = Math.max(1, Integer.parseInt(conf.getOrDefault("tasks.max", "1")));
        this.tasks = new AtomicReferenceArray<>(tasksMax);
        this.states = new AtomicReferenceArray<>(tasksMax);
        createTask(0); // 先试着创建一次：任务构造函数只读取并校验配置，配置有误时直接报错给调用方
    }

    public String name() {
        return name;
    }

    public Map<String, String> config() {
        return conf;
    }

    private BaseTask createTask(int taskId) {
        String cls = conf.getOrDefault("connector.class", "");
        switch (cls) {
            case "KafkaSourceConnector":
                return new PullerTask(name, taskId, conf);
            case "TransformSinkConnector":
                return new CleanTask(name, taskId, conf);
            case "MysqlSinkConnector":
                return new MysqlSinkTask(name, taskId, conf);
            default:
                throw new IllegalArgumentException("未知的 connector.class：" + cls);
        }
    }

    public void start() {
        for (int i = 0; i < tasksMax; i++) {
            final int taskId = i;
            Thread t = new Thread(() -> runTask(taskId), name + "-" + taskId);
            t.start();
            threads.add(t);
        }
        log.info("connector {} 启动 {} 个任务", name, tasksMax);
    }

    private void runTask(int taskId) {
        while (!stopped) {
            BaseTask task = createTask(taskId);
            tasks.set(taskId, task);
            if (stopped) {
                break; // 刚好在创建期间收到停止信号
            }
            states.set(taskId, "RUNNING");
            try {
                task.execute();
            } catch (Exception e) {
                states.set(taskId, "FAILED: " + e);
                log.error("任务 {} 失败，{} 毫秒后重启", task.name(), RESTART_BACKOFF_MS, e);
                sleepUnlessStopped(RESTART_BACKOFF_MS);
            }
        }
        states.set(taskId, "STOPPED");
    }

    public void stop() {
        stopped = true;
        for (int i = 0; i < tasksMax; i++) {
            BaseTask task = tasks.get(i);
            if (task != null) {
                task.stop();
            }
        }
        for (Thread t : threads) {
            try {
                t.join(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("connector {} 已停止", name);
    }

    public List<String> taskStates() {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < tasksMax; i++) {
            result.add(states.get(i) == null ? "UNASSIGNED" : states.get(i));
        }
        return result;
    }

    public Map<String, Object> info() {
        List<Map<String, Object>> taskInfos = new ArrayList<>();
        for (int i = 0; i < tasksMax; i++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", i);
            item.put("state", states.get(i) == null ? "UNASSIGNED" : states.get(i));
            BaseTask task = tasks.get(i);
            item.put("metrics", task == null ? Map.of() : task.metrics().snapshot());
            taskInfos.add(item);
        }
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", name);
        info.put("config", maskPassword(conf));
        info.put("tasks", taskInfos);
        return info;
    }

    /** 接口返回时隐藏密码（文档 6.6） */
    private static Map<String, String> maskPassword(Map<String, String> conf) {
        Map<String, String> copy = new LinkedHashMap<>(conf);
        copy.replaceAll((k, v) -> k.contains("password") ? "******" : v);
        return copy;
    }

    private void sleepUnlessStopped(long ms) {
        long end = System.currentTimeMillis() + ms;
        while (!stopped && System.currentTimeMillis() < end) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
