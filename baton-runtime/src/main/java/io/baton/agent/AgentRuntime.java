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
package io.baton.agent;

import io.baton.NodeId;
import io.baton.RemoteCallable;
import io.baton.RemoteRunnable;
import io.baton.transport.HttpTransport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/** Owns an agent's server, execution pool, registration, heartbeat, and shutdown. */
public final class AgentRuntime implements AutoCloseable {
    private final NodeId nodeId;
    private final JobRunner runner;
    private final AgentServer server;
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private HeartbeatReporter heartbeat;
    private boolean started;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private boolean closed;

    /** Bind the server and establish identity without registering or accepting jobs. */
    public AgentRuntime(int port, String name, String hostname) throws IOException {
        // A shutdown request may arrive as soon as the HTTP server binds.
        synchronized (this) {
            runner = new JobRunner(runId, name);
            try {
                server = new AgentServer(port, runner, this::close);
            } catch (IOException | RuntimeException e) {
                runner.shutdown();
                throw e;
            }
            nodeId = new NodeId(name, hostname, server.getPort(), (int) ProcessHandle.current().pid());
            runner.setNodeId(nodeId.toString());
        }
    }

    /** Publish the initialized agent, start heartbeats, and expose its ready identity. */
    public synchronized void start(String orchestratorUrl) throws IOException {
        if (closed) throw new IllegalStateException("Agent closed");
        if (started) throw new IllegalStateException("Agent already started");
        runner.connect(orchestratorUrl);
        heartbeat = new HeartbeatReporter(orchestratorUrl, nodeId, runId, this::close);
        try {
            // The orchestrator can dispatch immediately upon seeing registration,
            // before its HTTP response reaches us. Initializers have completed.
            server.acceptJobs();
            HttpTransport.post(orchestratorUrl + "/agent/register",
                    HeartbeatReporter.nodeIdToString(nodeId).getBytes(StandardCharsets.UTF_8),
                    "text/plain; charset=utf-8", HttpTransport.DEFAULT_TIMEOUT_MS);
            heartbeat.start();
            server.ready(nodeId);
            started = true;
        } catch (IOException | RuntimeException e) {
            close();
            throw e;
        }
    }

    public NodeId getNodeId() { return nodeId; }

    public synchronized Future<Void> execute(RemoteRunnable job) {
        requireStarted();
        return runner.submit(job);
    }

    public synchronized <R> Future<R> execute(RemoteCallable<R> job) {
        requireStarted();
        return runner.submit(job);
    }

    private void requireStarted() {
        if (!started || closed) throw new IllegalStateException("Agent is not running");
    }

    public void awaitShutdown() throws InterruptedException { stopped.await(); }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            if (heartbeat != null) heartbeat.stop();
            server.stop();
        } finally {
            stopped.countDown();
        }
    }
}
