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
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Main {@link Fabric} implementation.
 *
 * <p><b>Phase 1 (current):</b> all jobs execute in local threads — no network needed.
 * Distributed primitives are backed by in-process {@link PrimitivesStore}.
 * Use {@code connectLocal()} to add more "nodes" (they share the same JVM).
 *
 * <p><b>Phase 2 (TODO):</b> {@link BatonServer} routes are wired up; agents
 * run in child processes and communicate via HTTP.
 */
public class BatonFabric implements Fabric {

    private final NodeId             localNodeId;
    private final PrimitivesStore    primitives  = new PrimitivesStore();
    private final BarrierCoordinator barriers    = new BarrierCoordinator();
    private final QueueStore         queues      = new QueueStore();
    private final AgentRegistry      registry    = new AgentRegistry();
    private final JobDispatcher      dispatcher  = new JobDispatcher();
    private final ExecutorService    localPool   = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "baton-local-worker");
        t.setDaemon(true);
        return t;
    });

    private final BatonServer server; // null in unit-test / no-network mode

    public BatonFabric(int orchestratorPort) {
        this.localNodeId = buildLocalNodeId();
        BatonServer s = null;
        try {
            s = new BatonServer(primitives, barriers, queues, registry, dispatcher, orchestratorPort);
        } catch (IOException e) {
            System.err.println("[baton] WARNING: could not start HTTP server — running in local-only mode. " + e.getMessage());
        }
        this.server = s;
    }

    // ── Node lifecycle ─────────────────────────────────────────────────────────

    @Override
    public NodeId getLocalNodeId() { return localNodeId; }

    @Override
    public NodeId deployAndConnect(String hostname, SshConfig ssh) {
        // TODO Phase 5: delegate to AgentDeployer (baton-deployer module)
        throw new UnsupportedOperationException("SSH deployment not yet implemented — use connectLocal() for now");
    }

    @Override
    public NodeId connectLocal() {
        // Phase 1: spawn a local "node" in the same JVM (useful for unit tests)
        NodeId node = new NodeId("local-" + System.nanoTime(), localNodeId.getHostname(), 0, (int) ProcessHandle.current().pid());
        registry.register(node);
        return node;
    }

    @Override
    public Collection<NodeId> getConnectedNodes() { return registry.getConnectedNodes(); }

    @Override
    public void disconnect(NodeId node) { registry.remove(node); }

    // ── Remote execution ───────────────────────────────────────────────────────

    @Override
    public Future<Void> executeAsync(NodeId node, RemoteRunnable job) {
        if (isLocalNode(node)) {
            // Phase 1: run in a local thread
            return localPool.submit(() -> { job.run(); return null; });
        }
        // Phase 2: HTTP dispatch
        return dispatcher.dispatchRunnable(node, job);
    }

    @Override
    public <R> Future<R> executeAsync(NodeId node, RemoteCallable<R> job) {
        if (isLocalNode(node)) {
            return localPool.submit(job::call);
        }
        return dispatcher.dispatchCallable(node, job);
    }

    // ── Distributed primitives ─────────────────────────────────────────────────

    @Override
    public DistributedCounter counter(String name, long initialValue) {
        return primitives.getOrCreateCounter(name, initialValue);
    }

    @Override
    public DistributedBoolean bool(String name, boolean initialValue) {
        return primitives.getOrCreateBoolean(name, initialValue);
    }

    @Override
    public <T extends Serializable> DistributedReference<T> reference(String name, T initialValue) {
        return primitives.getOrCreateReference(name, initialValue);
    }

    @Override
    public DistributedBarrier barrier(String name, int parties) {
        return barriers.getOrCreate(name, parties);
    }

    @Override
    public <T extends Serializable> DistributedQueue<T> queue(String name) {
        return queues.getOrCreate(name);
    }

    // ── File transfer ──────────────────────────────────────────────────────────

    @Override
    public void upload(NodeId node, Path localPath, String remotePath) {
        // TODO Phase 5
        throw new UnsupportedOperationException("File upload not yet implemented");
    }

    @Override
    public void download(NodeId node, String remotePath, Path localDest) {
        // TODO Phase 5
        throw new UnsupportedOperationException("File download not yet implemented");
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    @Override
    public void close() {
        localPool.shutdownNow();
        registry.shutdown();
        dispatcher.failAll(new IllegalStateException("Fabric closed"));
        if (server != null) server.stop();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private boolean isLocalNode(NodeId node) {
        return registry.getConnectedNodes().contains(node)
                || node.equals(localNodeId);
    }

    private static NodeId buildLocalNodeId() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }
        int pid = (int) ProcessHandle.current().pid(); // ProcessHandle.pid() returns long
        return new NodeId("orchestrator", hostname, 0, pid);
    }
}
