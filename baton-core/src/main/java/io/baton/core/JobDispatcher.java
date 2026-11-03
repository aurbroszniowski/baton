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
import io.baton.transport.HttpTransport;
import io.baton.transport.Serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Predicate;

/**
 * Serializes lambdas into {@link ClassBundle}s and dispatches them to remote agents via HTTP.
 *
 * <p>Each job gets a unique {@code jobId}.  When the agent finishes it POSTs the result
 * back to {@code POST /agent/result/{jobId}} on the orchestrator.  The orchestrator's
 * HTTP handler calls {@link #completeJob} which resolves the {@link CompletableFuture}
 * returned to the caller.
 */
class JobDispatcher {

    private final ConcurrentHashMap<String, PendingJob> pending = new ConcurrentHashMap<>();
    private final Predicate<NodeId> isAlive;
    private volatile boolean closed;

    JobDispatcher() { this(node -> true); }
    JobDispatcher(Predicate<NodeId> isAlive) { this.isAlive = isAlive; }

    private static final class PendingJob {
        final NodeId node;
        final CompletableFuture<Object> future = new CompletableFuture<>();
        PendingJob(NodeId node) { this.node = node; }
    }

    /** Correlation ID for log lines — set by {@link BatonFabric} after construction. */
    private volatile String runId = "-";

    void setRunId(String runId) { this.runId = runId; }

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
        PendingJob job = pending.remove(jobId);
        if (job == null) return; // already timed out or duplicate delivery

        CompletableFuture<Object> future = job.future;

        System.out.printf("[baton][%s][%s] Job completed (exception=%b)%n", runId, jobId, isException);
        try {
            Object result = Serialization.read(resultBytes);
            if (isException) {
                future.completeExceptionally((Throwable) result);
            } else {
                future.complete(result);
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    /** Fail all pending jobs for a specific node — called when an agent is declared dead. */
    void failAgent(NodeId node, Throwable cause) {
        pending.forEach((id, job) -> {
            if (job.node.equals(node) && pending.remove(id, job)) job.future.completeExceptionally(cause);
        });
    }

    /** Fail all pending jobs and reject further submissions when the fabric closes. */
    void failAll(Throwable cause) {
        closed = true;
        pending.forEach((id, job) -> {
            if (pending.remove(id, job)) job.future.completeExceptionally(cause);
        });
    }

    // Private helpers 

    private CompletableFuture<Object> dispatch(NodeId node, Serializable job) {
        String jobId = UUID.randomUUID().toString();
        PendingJob pendingJob = new PendingJob(node);
        CompletableFuture<Object> future = pendingJob.future;
        pending.put(jobId, pendingJob);
        future.whenComplete((value, failure) -> pending.remove(jobId, pendingJob));

        System.out.printf("[baton][%s][%s] Dispatching job to %s%n", runId, jobId, node);
        try {
            if (closed) throw new IllegalStateException("Fabric closed");
            if (!isAlive.test(node)) throw new AgentDeadException("Agent is not connected: " + node);
            ClassBundle bundle = buildBundle(job);
            byte[] payload = serializePayload(jobId, bundle);
            String agentUrl = "http://" + node.getHostname() + ":" + node.getPort() + "/job";
            HttpTransport.post(agentUrl, payload);
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

}
