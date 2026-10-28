package com.example.databus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * 数据面进程入口：一个极简版的 Kafka Connect Worker。
 * <p>
 * 原平台中，Python 控制面通过 Kafka Connect 的 REST 接口下发任务（文档 18.7.5）：
 * PUT    /connectors/{name}/config   创建或更新 connector（更新时先停掉旧任务，再按新配置启动）
 * GET    /connectors                 列出所有 connector
 * GET    /connectors/{name}          查看配置和每个任务的状态、指标
 * DELETE /connectors/{name}          停止并删除 connector
 * <p>
 * 这里用 JDK 自带的 HttpServer 实现同样的接口，不依赖 Web 框架。
 * connector 配置保存在本地 JSON 文件中，Worker 重启后会自动恢复（真实 Kafka Connect 存在 config topic 里）。
 */
public class Worker {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
    };
    private static final TypeReference<Map<String, Map<String, String>>> STATE_TYPE =
            new TypeReference<Map<String, Map<String, String>>>() {
            };

    private final Map<String, Connector> connectors = new LinkedHashMap<>();
    private final File stateFile;

    Worker(File stateFile) {
        this.stateFile = stateFile;
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("WORKER_PORT", "8083"));
        File state = new File(System.getenv().getOrDefault("WORKER_STATE_FILE", "connectors.json"));

        // 先占端口再恢复任务：端口被占用（通常是旧 Worker 还在运行）时直接退出，不启动任何任务线程
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (BindException e) {
            log.error("端口 {} 已被占用，可能已有 Worker 在运行；先停掉它，或用 WORKER_PORT 换端口", port);
            System.exit(1);
            return;
        }

        Worker worker = new Worker(state);
        worker.restore();

        server.createContext("/connectors", worker::handle);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        log.info("Worker 已启动，REST 端口 {}，配置文件 {}", port, state.getAbsolutePath());

        // Ctrl+C 时让每个任务先 flush 再退出，保证已处理的数据都写出去并提交 offset
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0);
            worker.stopAll();
        }, "shutdown"));
    }

    // ------------------------- connector 管理 -------------------------

    synchronized void putConnector(String name, Map<String, String> conf) {
        Connector old = connectors.remove(name);
        if (old != null) {
            log.info("connector {} 已存在，先停止旧任务再按新配置启动", name);
            old.stop();
        }
        Connector connector = new Connector(name, conf); // 配置不合法会在这里抛异常
        connector.start();
        connectors.put(name, connector);
        save();
    }

    synchronized boolean deleteConnector(String name) {
        Connector connector = connectors.remove(name);
        if (connector == null) {
            return false;
        }
        connector.stop();
        save();
        return true;
    }

    synchronized Map<String, Object> info(String name) {
        Connector connector = connectors.get(name);
        return connector == null ? null : connector.info();
    }

    synchronized List<Map<String, Object>> list() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Connector c : connectors.values()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", c.name());
            item.put("connector.class", c.config().get("connector.class"));
            item.put("tasks", c.taskStates());
            result.add(item);
        }
        return result;
    }

    /**
     * 进程退出时调用：只停止任务，不删除配置，下次启动会恢复
     */
    synchronized void stopAll() {
        for (Connector c : connectors.values()) {
            c.stop();
        }
    }

    private void save() {
        Map<String, Map<String, String>> state = new LinkedHashMap<>();
        connectors.forEach((name, c) -> state.put(name, c.config()));
        try {
            JSON.writerWithDefaultPrettyPrinter().writeValue(stateFile, state);
        } catch (IOException e) {
            log.error("保存 connector 配置失败", e);
        }
    }

    private void restore() throws IOException {
        if (!stateFile.exists()) {
            return;
        }
        Map<String, Map<String, String>> state = JSON.readValue(stateFile, STATE_TYPE);
        for (Map.Entry<String, Map<String, String>> e : state.entrySet()) {
            try {
                putConnector(e.getKey(), e.getValue());
                log.info("恢复 connector {}", e.getKey());
            } catch (RuntimeException ex) {
                log.error("恢复 connector {} 失败", e.getKey(), ex);
            }
        }
    }

    // ------------------------- REST 接口 -------------------------

    private void handle(HttpExchange ex) throws IOException {
        try {
            // "/connectors/abc/config" -> ["", "connectors", "abc", "config"]
            String[] parts = ex.getRequestURI().getPath().replaceAll("/+$", "").split("/");
            String method = ex.getRequestMethod();

            if (parts.length == 2 && "GET".equals(method)) {
                reply(ex, 200, list());
                return;
            }
            if (parts.length < 3) {
                reply(ex, 404, Collections.singletonMap("error", "not found"));
                return;
            }

            String name = parts[2];
            if (parts.length == 4 && "config".equals(parts[3]) && "PUT".equals(method)) {
                Map<String, Object> body = JSON.readValue(ex.getRequestBody(), MAP_TYPE);
                Map<String, String> conf = new LinkedHashMap<>();
                body.forEach((k, v) -> conf.put(k, String.valueOf(v)));
                putConnector(name, conf);
                reply(ex, 200, info(name));
            } else if (parts.length == 3 && "GET".equals(method)) {
                Map<String, Object> info = info(name);
                reply(ex, info == null ? 404 : 200, info == null ? Collections.singletonMap("error", "connector 不存在") : info);
            } else if (parts.length == 3 && "DELETE".equals(method)) {
                boolean deleted = deleteConnector(name);
                reply(ex, deleted ? 200 : 404, Collections.singletonMap("deleted", deleted));
            } else {
                reply(ex, 404, Collections.singletonMap("error", "not found"));
            }
        } catch (Exception e) {
            log.warn("处理请求失败：{}", e.toString());
            reply(ex, 400, Collections.singletonMap("error", String.valueOf(e.getMessage())));
        }
    }

    private static void reply(HttpExchange ex, int code, Object body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
