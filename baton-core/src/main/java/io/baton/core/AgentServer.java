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
package io.baton.core;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The agent's HTTP server.
 *
 * <pre>
 * POST /job          <- receives ClassBundle, dispatches to JobRunner
 * GET  /health       <- liveness probe
 * POST /shutdown     <- graceful stop
 * POST /files        <- receive uploaded files (Phase 5)
 * GET  /files/{path} <- serve files for download (Phase 5)
 * </pre>
 */
public class AgentServer {

    private final HttpServer    server;
    private final JobRunner     runner;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Runnable      shutdownHook;

    public AgentServer(int port, JobRunner runner, Runnable shutdownHook) throws IOException {
        this.runner       = runner;
        this.shutdownHook = shutdownHook;

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "baton-agent-worker");
            t.setDaemon(true);
            return t;
        }));

        server.createContext("/job",      this::handleJob);
        server.createContext("/health",   this::handleHealth);
        server.createContext("/shutdown", this::handleShutdown);
        server.createContext("/files",    this::handleFiles);

        server.start();
    }

    public int getPort() { return server.getAddress().getPort(); }

    public void stop() {
        running.set(false);
        server.stop(1);
        runner.shutdown();
    }

    // Handlers 

    private void handleJob(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        byte[] payload = readBodyBytes(ex);
        runner.accept(payload); // async — returns immediately
        sendText(ex, 200, "accepted");
    }

    private void handleHealth(HttpExchange ex) throws IOException {
        sendText(ex, 200, running.get() ? "UP" : "DRAINING");
    }

    private void handleShutdown(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        sendText(ex, 200, "stopping");
        // Stop asynchronously so we can finish sending the response first
        Thread t = new Thread(shutdownHook, "baton-shutdown");
        t.setDaemon(false);
        t.start();
    }

    private void handleFiles(HttpExchange ex) throws IOException {
        // Extract ?path=<encoded-remote-path> from query string
        String query = ex.getRequestURI().getQuery();
        String remotePath = null;
        if (query != null) {
            for (String param : query.split("&")) {
                if (param.startsWith("path=")) {
                    remotePath = URLDecoder.decode(param.substring(5), StandardCharsets.UTF_8.name());
                    break;
                }
            }
        }
        if (remotePath == null) { sendStatus(ex, 400); return; }

        // Expand leading ~ to the user's home directory
        Path path = Path.of(remotePath.replaceFirst("^~", System.getProperty("user.home")));

        String method = ex.getRequestMethod().toUpperCase();
        if ("POST".equals(method)) {
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            byte[] bytes = readBodyBytes(ex);
            Files.write(path, bytes);
            sendText(ex, 200, "ok");
        } else if ("GET".equals(method)) {
            byte[] bytes = Files.readAllBytes(path);
            ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
            ex.sendResponseHeaders(200, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.close();
        } else {
            sendStatus(ex, 405);
        }
    }

    // Helpers 

    private byte[] readBodyBytes(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] block = new byte[8192];
            int n;
            while ((n = in.read(block)) != -1) buf.write(block, 0, n);
            return buf.toByteArray();
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
