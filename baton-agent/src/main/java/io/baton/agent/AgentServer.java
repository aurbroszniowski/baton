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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The agent's HTTP server.
 *
 * <pre>
 * POST /job          ← receives ClassBundle, dispatches to JobRunner
 * GET  /health       ← liveness probe
 * POST /shutdown     ← graceful stop
 * POST /files        ← receive uploaded files
 * GET  /files/{path} ← serve files for download
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

    private void handleJob(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        byte[] payload = readBodyBytes(ex);
        runner.accept(payload);
        sendText(ex, 200, "accepted");
    }

    private void handleHealth(HttpExchange ex) throws IOException {
        sendText(ex, 200, running.get() ? "UP" : "DRAINING");
    }

    private void handleShutdown(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        sendText(ex, 200, "stopping");
        Thread t = new Thread(shutdownHook, "baton-shutdown");
        t.setDaemon(false);
        t.start();
    }

    private void handleFiles(HttpExchange ex) throws IOException {
        String query = ex.getRequestURI().getQuery();
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
        if (remotePath.startsWith("angela-root://")) {
            String angelaRoot = System.getProperty("angela.rootDir",
                    System.getProperty("user.home") + "/.angela");
            path = Path.of(angelaRoot).resolve(remotePath.substring("angela-root://".length()));
        } else {
            path = Path.of(remotePath.replaceFirst("^~", System.getProperty("user.home")));
        }

        String method = ex.getRequestMethod().toUpperCase();
        if ("POST".equals(method)) {
            if (path.getParent() != null) Files.createDirectories(path.getParent());
            try (InputStream in = ex.getRequestBody();
                 OutputStream out = Files.newOutputStream(path)) {
                pipe(in, out);
            }
            if (modeParam != null) {
                try {
                    Files.setPosixFilePermissions(path, intToPosix(Integer.parseInt(modeParam, 8)));
                } catch (UnsupportedOperationException ignored) {}
            }
            sendText(ex, 200, "ok");
        } else if ("GET".equals(method)) {
            long size = Files.size(path);
            ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
            ex.sendResponseHeaders(200, size);
            try (InputStream in = Files.newInputStream(path);
                 OutputStream out = ex.getResponseBody()) {
                pipe(in, out);
            }
            ex.close();
        } else {
            sendStatus(ex, 405);
        }
    }

    private static Set<PosixFilePermission> intToPosix(int mode) {
        Set<PosixFilePermission> perms = EnumSet.noneOf(PosixFilePermission.class);
        if ((mode & 0400) != 0) perms.add(PosixFilePermission.OWNER_READ);
        if ((mode & 0200) != 0) perms.add(PosixFilePermission.OWNER_WRITE);
        if ((mode & 0100) != 0) perms.add(PosixFilePermission.OWNER_EXECUTE);
        if ((mode & 0040) != 0) perms.add(PosixFilePermission.GROUP_READ);
        if ((mode & 0020) != 0) perms.add(PosixFilePermission.GROUP_WRITE);
        if ((mode & 0010) != 0) perms.add(PosixFilePermission.GROUP_EXECUTE);
        if ((mode & 0004) != 0) perms.add(PosixFilePermission.OTHERS_READ);
        if ((mode & 0002) != 0) perms.add(PosixFilePermission.OTHERS_WRITE);
        if ((mode & 0001) != 0) perms.add(PosixFilePermission.OTHERS_EXECUTE);
        return perms;
    }

    private static void pipe(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[64 * 1024];
        int n;
        while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
    }

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
