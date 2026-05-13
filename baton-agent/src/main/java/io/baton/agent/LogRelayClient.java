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

import io.baton.AgentLogRelay;
import io.baton.RemoteExecutionContext;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Agent-side log relay.
 *
 * <p>Accepts log lines from producers (child-process readers, System.out/err
 * capture, framework bridges), buffers them in a bounded queue, and
 * periodically flushes batches to the orchestrator via {@code POST /agent/logs}.
 *
 * <p>All public methods are thread-safe.  {@link #submit} is non-blocking:
 * when the queue is full the record is silently dropped and the drop counter
 * is incremented.  Relay failures are logged locally and never propagate to
 * job threads.
 */
public class LogRelayClient implements AgentLogRelay {

    private static final int    QUEUE_CAPACITY    = 4_000;
    private static final int    FLUSH_INTERVAL_MS = 200;
    private static final int    CONNECT_TIMEOUT   = 3_000;
    private static final int    READ_TIMEOUT      = 5_000;

    // Internal record 

    private static final class Entry {
        final String nodeId, jobId, source, label, stream, line;

        Entry(String nodeId, String jobId, String source,
              String label, String stream, String line) {
            this.nodeId  = nodeId;
            this.jobId   = jobId;
            this.source  = source;
            this.label   = label;
            this.stream  = stream;
            this.line    = line;
        }

        /** Groups records with identical metadata into one HTTP POST. */
        String metaKey() {
            return nodeId + "|" + jobId + "|" + source + "|" + label + "|" + stream;
        }
    }

    // State 

    private final String                    orchestratorUrl;
    private final String                    selfNodeId;
    private final BlockingQueue<Entry>      queue     = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicLong                drops     = new AtomicLong();
    private final ScheduledExecutorService  scheduler;

    // Construction / lifecycle 

    /**
     * @param orchestratorUrl base URL of the Baton orchestrator, e.g. {@code "http://host:9400"}
     * @param selfNodeId      full NodeId string of this agent, used as {@code X-Node-Id}
     */
    public LogRelayClient(String orchestratorUrl, String selfNodeId) {
        this.orchestratorUrl = orchestratorUrl;
        this.selfNodeId      = selfNodeId;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "baton-log-relay");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start the periodic flush scheduler.  Call once after construction,
     * before submitting any records.
     */
    public void start() {
        scheduler.scheduleWithFixedDelay(
                this::flush, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the flush scheduler and perform a final drain/flush.
     * Safe to call more than once.
     */
    public void stop() {
        scheduler.shutdownNow();
        flush(); // drain anything remaining
    }

    // Producer API 

    /**
     * Submit a log line for relay.  Non-blocking: if the internal queue is
     * full the record is dropped and the drop counter is incremented.
     *
     * @param jobId   job identifier, or {@code "-"} if outside a job
     * @param source  producer type: {@code "process"}, {@code "system-out"},
     *                {@code "system-err"}, or {@code "framework"}
     * @param label   process name or logger name; {@code "-"} if not applicable
     * @param stream  stream: {@code "stdout"}, {@code "stderr"}, or {@code "-"}
     * @param line    the log line text
     */
    public void submit(String jobId, String source, String label, String stream, String line) {
        Entry e = new Entry(selfNodeId, jobId != null ? jobId : "-", source, label, stream, line);
        if (!queue.offer(e)) {
            drops.incrementAndGet();
        }
    }

    /**
     * Submit a log line using the current {@link RemoteExecutionContext} as the
     * job identifier.  If no context is active (e.g. called from outside a job),
     * {@code "-"} is used as the job ID.
     *
     * <p>This is the preferred overload for code running inside a Baton job, since
     * it avoids the need to thread the job ID through every call site.
     *
     * @param source  producer type: {@code "process"}, {@code "system-out"},
     *                {@code "system-err"}, or {@code "framework"}
     * @param label   process name or logger name; {@code "-"} if not applicable
     * @param stream  stream: {@code "stdout"}, {@code "stderr"}, or {@code "-"}
     * @param line    the log line text
     */
    public void submit(String source, String label, String stream, String line) {
        String jobId = RemoteExecutionContext.jobId();
        submit(jobId != null ? jobId : "-", source, label, stream, line);
    }

    /** Returns the number of records dropped due to queue overflow. */
    public long getDropCount() {
        return drops.get();
    }

    // Flush 

    private void flush() {
        if (queue.isEmpty()) return;

        List<Entry> batch = new ArrayList<>(Math.min(queue.size() + 1, 256));
        queue.drainTo(batch);
        if (batch.isEmpty()) return;

        // Group by metadata so records with the same source share one POST.
        Map<String, List<Entry>> groups = new LinkedHashMap<>();
        for (Entry e : batch) {
            groups.computeIfAbsent(e.metaKey(), k -> new ArrayList<>()).add(e);
        }
        for (List<Entry> group : groups.values()) {
            sendBatch(group);
        }
    }

    private void sendBatch(List<Entry> group) {
        Entry first = group.get(0);

        StringBuilder body = new StringBuilder();
        for (Entry e : group) {
            body.append(e.line).append('\n');
        }

        try {
            HttpURLConnection conn =
                    (HttpURLConnection) new URL(orchestratorUrl + "/agent/logs").openConnection();
            conn.setConnectTimeout(CONNECT_TIMEOUT);
            conn.setReadTimeout(READ_TIMEOUT);
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            conn.setRequestProperty("X-Node-Id", first.nodeId);
            conn.setRequestProperty("X-Job-Id",  first.jobId);
            conn.setRequestProperty("X-Source",  first.source);
            conn.setRequestProperty("X-Label",   first.label);
            conn.setRequestProperty("X-Stream",  first.stream);
            byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
            conn.setRequestProperty("Content-Length", Integer.toString(bytes.length));
            try (OutputStream out = conn.getOutputStream()) {
                out.write(bytes);
            }
            int code = conn.getResponseCode();
            if (code != 200) {
                System.err.printf("[baton-log-relay] POST /agent/logs returned HTTP %d%n", code);
            }
        } catch (IOException e) {
            // Best-effort: log locally and move on — never block or throw
            System.err.printf("[baton-log-relay] Failed to relay %d log lines: %s%n",
                    group.size(), e.getMessage());
        }
    }
}
