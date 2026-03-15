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

import io.baton.DistributedBarrier;
import io.baton.DistributedBoolean;
import io.baton.DistributedCounter;
import io.baton.DistributedQueue;
import io.baton.DistributedReference;
import io.baton.Fabric;
import io.baton.NodeId;
import io.baton.RemoteCallable;
import io.baton.RemoteRunnable;
import io.baton.SshConfig;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Fabric} implementation for agents connecting to an existing Baton orchestrator.
 *
 * <p>The agent:
 * <ol>
 *   <li>Starts an {@link AgentServer} on {@code agentPort} to receive job dispatches.
 *   <li>Registers itself with the orchestrator via {@code POST /agent/register}.
 *   <li>Starts a heartbeat thread to keep the registration alive.
 *   <li>Executes jobs dispatched to its own {@link NodeId} in a local thread pool.
 * </ol>
 *
 * <p>Distributed primitives delegate to the orchestrator via HTTP proxies.
 */
public class BatonAgentFabric implements Fabric {

    private final NodeId          localNodeId;
    private final String          orchestratorUrl;
    private final AgentServer     agentServer;
    private final HeartbeatReporter heartbeat;
    private final ExecutorService localPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "baton-agent-worker");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean   closed = new AtomicBoolean(false);
    private final String          agentRunId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);

    public BatonAgentFabric(String orchestratorUrl, int agentPort, String agentName) throws IOException {
        this.orchestratorUrl = orchestratorUrl;

        String hostname;
        try { hostname = InetAddress.getLocalHost().getHostName(); }
        catch (UnknownHostException e) { hostname = "localhost"; }
        int pid = (int) ProcessHandle.current().pid();

        JobRunner runner = new JobRunner(orchestratorUrl, agentRunId);

        Object shutdownLock = new Object();
        this.agentServer = new AgentServer(agentPort, runner, () -> {
            synchronized (shutdownLock) { shutdownLock.notifyAll(); }
        });

        int actualPort = this.agentServer.getPort();
        this.localNodeId = new NodeId(agentName, hostname, actualPort, pid);

        // Register with orchestrator
        register();

        // Start heartbeat — self-shutdown if orchestrator goes silent
        this.heartbeat = new HeartbeatReporter(orchestratorUrl, localNodeId, agentRunId, this::close);
        this.heartbeat.start();
    }

    @Override public NodeId getLocalNodeId() { return localNodeId; }

    @Override public int getOrchestratorPort() { return -1; } // agent doesn't run an orchestrator

    @Override public String getOrchestratorUrl() { return orchestratorUrl; }

    @Override
    public NodeId deployAndConnect(String hostname, SshConfig ssh) {
        throw new UnsupportedOperationException("Agent cannot deploy other agents");
    }

    @Override
    public NodeId connectLocal() {
        throw new UnsupportedOperationException("Agent cannot create local sub-nodes");
    }

    @Override
    public Collection<NodeId> getConnectedNodes() { return Collections.emptyList(); }

    @Override
    public void disconnect(NodeId node) { /* no-op for agent */ }

    @Override
    public Future<Void> executeAsync(NodeId node, RemoteRunnable job) {
        return localPool.submit(() -> { job.run(); return null; });
    }

    @Override
    public <R> Future<R> executeAsync(NodeId node, RemoteCallable<R> job) {
        return localPool.submit(job::call);
    }

    @Override
    public DistributedCounter counter(String name, long initialValue) {
        return new HttpCounterProxy(orchestratorUrl, name);
    }

    @Override
    public DistributedBoolean bool(String name, boolean initialValue) {
        return new HttpBooleanProxy(orchestratorUrl, name);
    }

    @Override
    public <T extends Serializable> DistributedReference<T> reference(String name, T initialValue) {
        return new HttpReferenceProxy<>(orchestratorUrl, name);
    }

    @Override
    public DistributedBarrier barrier(String name, int parties) {
        return new HttpBarrierProxy(orchestratorUrl, name, parties);
    }

    @Override
    public <T extends Serializable> DistributedQueue<T> queue(String name) {
        return new HttpQueueProxy<>(orchestratorUrl, name);
    }

    @Override
    public void upload(NodeId node, Path localPath, String remotePath) {
        // For local agent: direct file copy
        if (Files.isDirectory(localPath)) {
            try (var stream = Files.walk(localPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    String rel = localPath.relativize(file).toString().replace(java.io.File.separatorChar, '/');
                    upload(node, file, remotePath + "/" + rel);
                });
            } catch (java.io.IOException e) {
                throw new RuntimeException("Upload directory failed: " + e.getMessage(), e);
            }
            return;
        }
        try {
            Path target = Path.of(remotePath.replaceFirst("^~(/|$)", System.getProperty("user.home") + "/"));
            if (target.getParent() != null) Files.createDirectories(target.getParent());
            Files.copy(localPath, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Local upload failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void download(NodeId node, String remotePath, Path localDest) {
        try {
            Path source = Path.of(remotePath.replaceFirst("^~(/|$)", System.getProperty("user.home") + "/"));
            if (localDest.getParent() != null) Files.createDirectories(localDest.getParent());
            Files.copy(source, localDest, StandardCopyOption.REPLACE_EXISTING);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Local download failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        heartbeat.stop();
        agentServer.stop();
    }

    private void register() throws IOException {
        String body = HeartbeatReporter.nodeIdToString(localNodeId);
        HttpURLConnection conn = (HttpURLConnection) new URL(orchestratorUrl + "/agent/register").openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        try (OutputStream out = conn.getOutputStream()) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
        int code = conn.getResponseCode();
        if (code != 200) throw new IOException("Orchestrator rejected registration: HTTP " + code);
    }
}
