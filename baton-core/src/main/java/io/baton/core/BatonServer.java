/*
 * Copyright Aurelien Broszniowski
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
import io.baton.DistributedQueue;
import io.baton.DistributedReference;
import io.baton.NodeId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The orchestrator's HTTP server.
 *
 * <p>All distributed state (primitives, barriers, queues) lives here.  Agents call
 * back to these endpoints.
 *
 * <pre>
 * POST /agent/register                     ← agent startup notification
 * POST /agent/heartbeat                    ← keepalive (every 5 s)
 * POST /agent/result/{jobId}               ← job completion callback
 *
 * GET  /primitive/counter/{name}/get
 * POST /primitive/counter/{name}/increment
 * POST /primitive/counter/{name}/getAndSet?value={v}
 * POST /primitive/counter/{name}/cas?expect={e}&update={u}
 *
 * GET  /primitive/boolean/{name}/get
 * POST /primitive/boolean/{name}/set?value={v}
 * POST /primitive/boolean/{name}/cas?expect={e}&update={u}
 *
 * GET  /primitive/reference/{name}/get     ← base64-encoded serialized value
 * POST /primitive/reference/{name}/set     ← binary body = serialized value
 *
 * POST /barrier/{name}/arrive?count={N}&nodeId={id}&generation={g}   ← long-poll
 *
 * POST /transfer/{queueId}/push            ← binary body
 * GET  /transfer/{queueId}/pop             ← long-poll, binary body
 * </pre>
 */
class BatonServer {

    private final HttpServer         server;
    private final PrimitivesStore    primitives;
    private final BarrierCoordinator barriers;
    private final QueueStore         queues;
    private final AgentRegistry      registry;
    private final JobDispatcher      dispatcher;
    private final int                port;

    BatonServer(PrimitivesStore primitives, BarrierCoordinator barriers,
                    QueueStore queues, AgentRegistry registry,
                    JobDispatcher dispatcher, int requestedPort) throws IOException {
        this.primitives  = primitives;
        this.barriers    = barriers;
        this.queues      = queues;
        this.registry    = registry;
        this.dispatcher  = dispatcher;

        server = HttpServer.create(new InetSocketAddress(requestedPort), 0);
        port   = server.getAddress().getPort();

        // Use a cached-thread executor so barrier long-polls don't block other requests
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "baton-server-worker");
            t.setDaemon(true);
            return t;
        }));

        server.createContext("/agent/register",  this::handleAgentRegister);
        server.createContext("/agent/heartbeat", this::handleAgentHeartbeat);
        server.createContext("/agent/result/",   this::handleJobResult);
        server.createContext("/primitive/counter/",   this::handleCounter);
        server.createContext("/primitive/boolean/",   this::handleBoolean);
        server.createContext("/primitive/reference/", this::handleReference);
        server.createContext("/barrier/",        this::handleBarrier);
        server.createContext("/transfer/",       this::handleTransfer);

        server.start();
    }

    int getPort() { return port; }

    void stop() { server.stop(0); }

    // ── /agent/* ───────────────────────────────────────────────────────────────

    private void handleAgentRegister(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        // Body: "name hostname port pid"
        String[] parts = readBody(ex).split(" ");
        NodeId nodeId = new NodeId(parts[0], parts[1], Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
        registry.register(nodeId);
        sendText(ex, 200, "OK");
    }

    private void handleAgentHeartbeat(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        String[] parts = readBody(ex).split(" ");
        NodeId nodeId = new NodeId(parts[0], parts[1], Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
        registry.heartbeat(nodeId);
        sendText(ex, 200, "OK");
    }

    private void handleJobResult(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        // Path: /agent/result/{jobId}?exception=true|false
        String jobId = ex.getRequestURI().getPath().substring("/agent/result/".length());
        String query  = ex.getRequestURI().getQuery();
        boolean isException = query != null && query.contains("exception=true");
        byte[] body = readBodyBytes(ex);
        dispatcher.completeJob(jobId, body, isException);
        sendText(ex, 200, "OK");
    }

    // ── /primitive/counter/* ──────────────────────────────────────────────────

    private void handleCounter(HttpExchange ex) throws IOException {
        String path   = ex.getRequestURI().getPath(); // /primitive/counter/{name}/{op}
        String[] segs = path.split("/");
        // segs: ["", "primitive", "counter", "{name}", "{op}"]
        if (segs.length < 5) { sendStatus(ex, 400); return; }
        String name = segs[3];
        String op   = segs[4];
        long init   = queryLong(ex, "init", 0L);
        io.baton.DistributedCounter c = primitives.getOrCreateCounter(name, init);
        String result;
        switch (op) {
            case "get":           result = Long.toString(c.get());              break;
            case "increment":     result = Long.toString(c.incrementAndGet());  break;
            case "getAndIncrement": result = Long.toString(c.getAndIncrement()); break;
            case "getAndSet":     result = Long.toString(c.getAndSet(queryLong(ex, "value", 0L))); break;
            case "cas":           result = Boolean.toString(
                    c.compareAndSet(queryLong(ex, "expect", 0L), queryLong(ex, "update", 0L))); break;
            default: sendStatus(ex, 404); return;
        }
        sendText(ex, 200, result);
    }

    // ── /primitive/boolean/* ──────────────────────────────────────────────────

    private void handleBoolean(HttpExchange ex) throws IOException {
        String path   = ex.getRequestURI().getPath();
        String[] segs = path.split("/");
        if (segs.length < 5) { sendStatus(ex, 400); return; }
        String name = segs[3];
        String op   = segs[4];
        boolean init = queryBool(ex, "init", false);
        io.baton.DistributedBoolean b = primitives.getOrCreateBoolean(name, init);
        String result;
        switch (op) {
            case "get":       result = Boolean.toString(b.get());              break;
            case "set":       b.set(queryBool(ex, "value", false));
                              result = "OK";                                   break;
            case "getAndSet": result = Boolean.toString(b.getAndSet(queryBool(ex, "value", false))); break;
            case "cas":       result = Boolean.toString(
                    b.compareAndSet(queryBool(ex, "expect", false), queryBool(ex, "update", false))); break;
            default: sendStatus(ex, 404); return;
        }
        sendText(ex, 200, result);
    }

    // ── /primitive/reference/* ────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void handleReference(HttpExchange ex) throws IOException {
        String path   = ex.getRequestURI().getPath();
        String[] segs = path.split("/");
        // segs: ["", "primitive", "reference", "{name}", "{op}"]
        if (segs.length < 5) { sendStatus(ex, 400); return; }
        String name = segs[3];
        String op   = segs[4];

        switch (op) {
            case "get": {
                DistributedReference<Serializable> ref =
                        (DistributedReference<Serializable>) primitives.getOrCreateReference(name, (Serializable) null);
                byte[] bytes = serializeObject(ref.get());
                sendText(ex, 200, Base64.getEncoder().encodeToString(bytes));
                break;
            }
            case "set": {
                byte[] body = readBodyBytes(ex);
                Serializable value = (Serializable) deserializeObject(body);
                DistributedReference<Serializable> ref =
                        (DistributedReference<Serializable>) primitives.getOrCreateReference(name, value);
                ref.set(value);
                sendText(ex, 200, "OK");
                break;
            }
            case "cas": {
                byte[] body = readBodyBytes(ex);
                try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(body))) {
                    Serializable expected = (Serializable) ois.readObject();
                    Serializable update   = (Serializable) ois.readObject();
                    DistributedReference<Serializable> ref =
                            (DistributedReference<Serializable>) primitives.getOrCreateReference(name, (Serializable) null);
                    sendText(ex, 200, Boolean.toString(ref.compareAndSet(expected, update)));
                } catch (ClassNotFoundException e) {
                    sendStatus(ex, 400);
                }
                break;
            }
            default: sendStatus(ex, 404);
        }
    }

    // ── /barrier/* ────────────────────────────────────────────────────────────

    private void handleBarrier(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
        String path   = ex.getRequestURI().getPath(); // /barrier/{name}/arrive
        String[] segs = path.split("/");
        if (segs.length < 4) { sendStatus(ex, 400); return; }
        String name       = segs[2];
        int    count      = (int) queryLong(ex, "count",      2L);
        int    generation = (int) queryLong(ex, "generation", 0L);
        long   timeoutMs  = queryLong(ex, "timeoutMs", 60_000L);

        // Ensure barrier is registered before first arrive
        barriers.getOrCreate(name, count);
        try {
            int index = barriers.arrive(name, generation, timeoutMs);
            sendText(ex, 200, "{\"index\":" + index + ",\"generation\":" + generation + "}");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendText(ex, 503, "interrupted");
        } catch (java.util.concurrent.TimeoutException e) {
            sendText(ex, 408, "timeout");
        }
    }

    // ── /transfer/* ───────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void handleTransfer(HttpExchange ex) throws IOException {
        String path   = ex.getRequestURI().getPath(); // /transfer/{queueId}/push|pop
        String[] segs = path.split("/");
        // segs: ["", "transfer", "{queueId}", "push|pop"]
        if (segs.length < 4) { sendStatus(ex, 400); return; }
        String queueId = segs[2];
        String op      = segs[3];

        @SuppressWarnings({"unchecked", "rawtypes"})
        DistributedQueue<byte[]> q = (DistributedQueue<byte[]>) (DistributedQueue) queues.getOrCreate(queueId);

        switch (op) {
            case "push": {
                if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { sendStatus(ex, 405); return; }
                try {
                    q.put(readBodyBytes(ex));
                    sendText(ex, 200, "OK");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    sendStatus(ex, 503);
                }
                break;
            }
            case "pop": {
                long timeoutMs = queryLong(ex, "timeoutMs", 0L);
                try {
                    byte[] item = (timeoutMs > 0)
                            ? q.poll(timeoutMs, TimeUnit.MILLISECONDS)
                            : q.take();
                    if (item == null) {
                        sendStatus(ex, 204); // No Content — timeout, nothing to pop
                    } else {
                        ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                        ex.sendResponseHeaders(200, item.length);
                        ex.getResponseBody().write(item);
                        ex.close();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    sendStatus(ex, 503);
                }
                break;
            }
            default: sendStatus(ex, 404);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private byte[] serializeObject(Object obj) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(buf)) {
            oos.writeObject(obj);
        }
        return buf.toByteArray();
    }

    private Object deserializeObject(byte[] bytes) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            try {
                return ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Cannot deserialize object", e);
            }
        }
    }

    private String readBody(HttpExchange ex) throws IOException {
        return new String(readBodyBytes(ex), StandardCharsets.UTF_8).trim();
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

    private long queryLong(HttpExchange ex, String param, long defaultValue) {
        String q = ex.getRequestURI().getQuery();
        if (q == null) return defaultValue;
        for (String kv : q.split("&")) {
            if (kv.startsWith(param + "=")) {
                try { return Long.parseLong(kv.substring(param.length() + 1)); } catch (NumberFormatException ignored) {}
            }
        }
        return defaultValue;
    }

    private boolean queryBool(HttpExchange ex, String param, boolean defaultValue) {
        String q = ex.getRequestURI().getQuery();
        if (q == null) return defaultValue;
        for (String kv : q.split("&")) {
            if (kv.startsWith(param + "=")) return Boolean.parseBoolean(kv.substring(param.length() + 1));
        }
        return defaultValue;
    }
}
