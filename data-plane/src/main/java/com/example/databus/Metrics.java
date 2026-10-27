package com.example.databus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 极简指标：累计计数 + 每 10 秒打印一次速率（对应文档 15.4 / 18.12 的监控指标）。
 * 计数可能来自 Producer 回调线程，所以用线程安全的 LongAdder。
 */
public class Metrics {
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);
    private static final long REPORT_INTERVAL_MS = 10_000;

    private final String name;
    private final Map<String, LongAdder> counters = new ConcurrentSkipListMap<>();
    private final Map<String, Long> lastValues = new HashMap<>();
    private long lastReport = System.currentTimeMillis();

    public Metrics(String name) {
        this.name = name;
    }

    public void inc(String key) {
        inc(key, 1);
    }

    public void inc(String key, long n) {
        counters.computeIfAbsent(key, k -> new LongAdder()).add(n);
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> result = new TreeMap<>();
        counters.forEach((k, v) -> result.put(k, v.sum()));
        return result;
    }

    /** 由任务线程在主循环里调用 */
    public void maybeReport() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastReport;
        if (elapsed < REPORT_INTERVAL_MS || counters.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> e : snapshot().entrySet()) {
            long rate = (e.getValue() - lastValues.getOrDefault(e.getKey(), 0L)) * 1000 / elapsed;
            sb.append(e.getKey()).append('=').append(e.getValue()).append('(').append(rate).append("/s) ");
            lastValues.put(e.getKey(), e.getValue());
        }
        log.info("[{}] {}", name, sb.toString().trim());
        lastReport = now;
    }
}
