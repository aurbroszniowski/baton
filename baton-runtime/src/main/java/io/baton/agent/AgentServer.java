/*
 * Copyright Aurélien Broszniowski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.baton.agent;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.baton.NodeId;
import io.baton.transport.AgentFiles;
import io.baton.transport.Serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The agent's HTTP server.
 *
 * <pre>
 * POST /job          - receives ClassBundle, dispatches to JobRunner
 * GET  /health       - liveness probe
 * POST /shutdown     - graceful stop
 * POST /files        - receive uploaded files
 * GET  /files/{path} - serve files for download
 * </pre>
 */
public class AgentServer {

    private final HttpServer    server;
    private final ExecutorService executor;
    private final JobRunner     runner;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Runnable      shutdownHook;
    private volatile NodeId identity;
    private volatile boolean acceptingJobs;

    public AgentServer(int port, JobRunner runner, Runnable shutdownHook) throws IOException {
        this.runner       = runner;
        this.shutdownHook = shutdownHook;

        server = HttpServer.create(new InetSocketAddress(port), 0);
        executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "baton-agent-worker");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);

        server.createContext("/job",      this::handleJob);
        server.createContext("/health",   this::handleHealth);
        server.createContext("/identity", this::handleIdentity);
        server.createContext("/shutdown", this::handleShutdown);
        server.createContext("/files",    this::handleFiles);

        server.start();
    }

    public int getPort() { return server.getAddress().getPort(); }

    void ready(NodeId nodeId) { identity = nodeId; }

    void acceptJobs() { acceptingJobs = true; }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        server.stop(0);
        executor.shutdownNow();
        runner.shutdown();
    }

    private void handleJob(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        if (!acceptingJobs) { sendText(ex, 503, "Agent is initializing"); return; }
        byte[] payload = readBodyBytes(ex);
        runner.accept(payload);
        sendText(ex, 200, "accepted");
    }

    private void handleHealth(HttpExchange ex) throws IOException {
        sendText(ex, identity != null && running.get() ? 200 : 503, identity != null ? "UP" : "STARTING");
    }

    private void handleIdentity(HttpExchange ex) throws IOException {
        NodeId nodeId = identity;
        if (nodeId == null) { sendStatus(ex, 503); return; }
        byte[] bytes = Serialization.write(nodeId);
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = ex.getResponseBody()) { out.write(bytes); }
        finally { ex.close(); }
    }

    private void handleShutdown(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        sendText(ex, 200, "stopping");
        Thread t = new Thread(shutdownHook, "baton-shutdown");
        t.setDaemon(false);
        t.start();
    }

    private void handleFiles(HttpExchange ex) throws IOException {
        String query = ex.getRequestURI().getRawQuery();
        String remotePath = null;
        String modeParam = null;
        if (query != null) {
            for (String param : query.split("&")) {
                if (param.startsWith("path=")) {
                    remotePath = URLDecoder.decode(param.substring(5), StandardCharsets.UTF_8.name());
                } else if (param.startsWith("mode=")) {
                    modeParam = param.substring(5);
                }
            }
        }
        if (remotePath == null) { sendStatus(ex, 400); return; }

        Path path;
        try {
            path = AgentFiles.resolve(remotePath);
        } catch (IllegalArgumentException e) {
            sendText(ex, 400, e.getMessage());
            return;
        }

        String method = ex.getRequestMethod().toUpperCase();
        if ("POST".equals(method)) {
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            try (InputStream in = ex.getRequestBody();
                 OutputStream out = Files.newOutputStream(path)) {
                in.transferTo(out);
            }
            if (modeParam != null) {
                try {
                    Files.setPosixFilePermissions(path, AgentFiles.permissions(Integer.parseInt(modeParam, 8)));
                } catch (UnsupportedOperationException ignored) {}
            }
            sendText(ex, 200, "ok");
        } else if ("GET".equals(method)) {
            long size = Files.size(path);
            ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
            ex.sendResponseHeaders(200, size);
            try (InputStream in = Files.newInputStream(path);
                 OutputStream out = ex.getResponseBody()) {
                in.transferTo(out);
            }
            ex.close();
        } else {
            sendStatus(ex, 405);
        }
    }

    private byte[] readBodyBytes(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            return in.readAllBytes();
        }
    }

    private void sendText(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    private void sendStatus(HttpExchange ex, int status) throws IOException {
        ex.sendResponseHeaders(status, -1);
        ex.close();
    }
}
