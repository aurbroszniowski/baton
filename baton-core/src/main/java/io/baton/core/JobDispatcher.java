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

import io.baton.ClassBundle;
import io.baton.NodeId;
import io.baton.RemoteCallable;
import io.baton.RemoteRunnable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Serializes lambdas into {@link ClassBundle}s and dispatches them to remote agents via HTTP.
 *
 * <p>Each job gets a unique {@code jobId}.  When the agent finishes it POSTs the result
 * back to {@code POST /agent/result/{jobId}} on the orchestrator.  The orchestrator's
 * HTTP handler calls {@link #completeJob} which resolves the {@link CompletableFuture}
 * returned to the caller.
 */
class JobDispatcher {

    /** Pending futures keyed by jobId, resolved when the agent POSTs a result. */
    private final ConcurrentHashMap<String, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();

    Future<Void> dispatchRunnable(NodeId node, RemoteRunnable job) {
        return dispatch(node, job).thenApply(ignored -> null);
    }

    @SuppressWarnings("unchecked")
    <R> Future<R> dispatchCallable(NodeId node, RemoteCallable<R> job) {
        return (Future<R>) (Future<?>) dispatch(node, job);
    }

    /**
     * Called by the orchestrator HTTP server when an agent POSTs
     * {@code POST /agent/result/{jobId}}.
     *
     * @param jobId       the job identifier
     * @param resultBytes serialized result (or exception if {@code isException} is true)
     * @param isException whether the agent is reporting a failure
     */
    void completeJob(String jobId, byte[] resultBytes, boolean isException) {
        CompletableFuture<Object> future = pending.remove(jobId);
        if (future == null) return; // already timed out or duplicate delivery

        try {
            Object result = deserialize(resultBytes);
            if (isException) {
                future.completeExceptionally((Throwable) result);
            } else {
                future.complete(result);
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    /** Fail all pending jobs — used when the orchestrator shuts down or an agent dies. */
    void failAll(Throwable cause) {
        pending.values().forEach(f -> f.completeExceptionally(cause));
        pending.clear();
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private CompletableFuture<Object> dispatch(NodeId node, Serializable job) {
        String jobId = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pending.put(jobId, future);

        try {
            ClassBundle bundle = buildBundle(job);
            byte[] payload = serializePayload(jobId, bundle);
            String agentUrl = "http://" + node.getHostname() + ":" + node.getPort() + "/job";
            postBinary(agentUrl, payload);
        } catch (Exception e) {
            pending.remove(jobId);
            future.completeExceptionally(e);
        }

        return future;
    }

    private ClassBundle buildBundle(Serializable job) throws IOException {
        ByteArrayOutputStream lambdaBuf = new ByteArrayOutputStream();
        ClassCollector collector = new ClassCollector(lambdaBuf);
        collector.writeObject(job);
        collector.flush();
        return new ClassBundle(collector.getClassBytes(), lambdaBuf.toByteArray());
    }

    private byte[] serializePayload(String jobId, ClassBundle bundle) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeUTF(jobId);
            oos.writeObject(bundle);
        }
        return out.toByteArray();
    }

    private Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        }
    }

    private void postBinary(String url, byte[] data) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.getOutputStream().write(data);
        int code = conn.getResponseCode();
        if (code != 200) throw new IOException("Agent rejected job dispatch: HTTP " + code);
    }
}
