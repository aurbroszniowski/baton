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

import io.baton.AgentLauncher;
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
import java.io.File;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Enumeration;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

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

    /** Short run ID for correlating log lines from the same orchestrator instance. */
    private final String             runId       = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private NodeId                   localNodeId;
    private final PrimitivesStore    primitives  = new PrimitivesStore();
    private final BarrierCoordinator barriers    = new BarrierCoordinator();
    private final QueueStore         queues      = new QueueStore();
    private final AgentRegistry      registry    = new AgentRegistry();
    private final JobDispatcher      dispatcher  = new JobDispatcher();
    private final LogCollector       logCollector = new LogCollector(new PrintingLogSink(System.out));
    private final ExecutorService    localPool   = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "baton-local-worker");
        t.setDaemon(true);
        return t;
    });
    /** Nodes created by {@link #connectLocal()} — only these run in the local thread pool. */
    private final Set<NodeId>        localNodes  = ConcurrentHashMap.newKeySet();
    /** Guards against double-close. */
    private final AtomicBoolean      closed      = new AtomicBoolean(false);

    private final BatonServer server;  // null in unit-test / no-network mode
    private final String      baseUrl; // null in local-only mode

    public BatonFabric(int orchestratorPort) {
        this.localNodeId = buildLocalNodeId(0); // placeholder with port 0
        // Wire runId and death listener (field initializers have already run)
        dispatcher.setRunId(runId);
        registry.setRunId(runId);
        registry.setDeathListener(node -> dispatcher.failAgent(node,
                new AgentDeadException("Agent " + node + " missed heartbeat")));
        BatonServer s   = null;
        String      url = null;
        if (orchestratorPort >= 0) { // -1 = local-only mode, no HTTP server
            try {
                s   = new BatonServer(primitives, barriers, queues, registry, dispatcher, logCollector, orchestratorPort);
                url = "http://" + resolveOrchestratorHost() + ":" + s.getPort();
            } catch (IOException e) {
                System.err.printf("[baton][%s][-] WARNING: could not start HTTP server — %s%n", runId, e.getMessage());
            }
        }
        this.server  = s;
        this.baseUrl = url;
        if (server != null) {
            // Update with actual HTTP port AND the routable IP (same host used in baseUrl)
            // so that toNodeId(agentID).equals(localNodeId) holds when the orchestrator
            // AgentID is built from getOrchestratorUrl().  Without this, isLocalNode()
            // fails for localhost tests because getHostName() returns the machine name
            // (e.g. "MacBook-Pro.local") while the AgentID uses the routable IP.
            NodeId old = this.localNodeId;
            String routeableHost = resolveOrchestratorHost();
            this.localNodeId = new NodeId(old.getName(), routeableHost, server.getPort(), old.getPid());
        }
    }

    /** Returns the actual orchestrator port (useful when started with port 0), or -1 in local mode. */
    public int getOrchestratorPort() {
        return server != null ? server.getPort() : -1;
    }

    /**
     * Replace the log sink used to output remote agent log lines.
     * The default sink prints structured lines to {@code System.out}.
     */
    public void setLogSink(LogSink sink) {
        logCollector.setSink(sink);
    }

    /**
     * Enable ANSI colour on the default {@link PrintingLogSink}.
     * No-op if the sink has been replaced with a custom one.
     */
    public void enableAnsiColour() {
        logCollector.mapSink(s -> s instanceof PrintingLogSink
                ? ((PrintingLogSink) s).withAnsi()
                : s);
    }

    @Override
    public String getOrchestratorUrl() {
        return baseUrl;
    }

    // ── Node lifecycle ─────────────────────────────────────────────────────────

    @Override
    public NodeId getLocalNodeId() { return localNodeId; }

    @Override
    public NodeId deployAndConnect(String hostname, SshConfig ssh) {
        if (baseUrl == null) {
            throw new IllegalStateException("Fabric must be started with a port to support deployAndConnect");
        }
        AgentLauncher launcher = ServiceLoader.load(AgentLauncher.class)
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException(
                        "No AgentLauncher found on classpath — add baton-deployer as a dependency"));
        try {
            NodeId provisional = launcher.launch(hostname, ssh, baseUrl);
            // The deployer may return a NodeId with pid=-1 (placeholder). Look up the
            // actual registered NodeId (with real PID) so that jobId->node tracking in
            // JobDispatcher and failAgent() lookup stay consistent.
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                for (NodeId n : registry.getConnectedNodes()) {
                    if (n.getHostname().equals(provisional.getHostname())
                            && n.getPort() == provisional.getPort()) {
                        return n;
                    }
                }
                try { Thread.sleep(100); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return provisional; // fallback — agent may not have registered yet
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deploy agent on " + hostname, e);
        }
    }

    @Override
    public NodeId connectLocal() {
        // Spawn a local "node" in the same JVM (useful for unit tests)
        NodeId node = new NodeId("local-" + System.nanoTime(), localNodeId.getHostname(), 0, (int) ProcessHandle.current().pid());
        localNodes.add(node); // track separately so remote agents aren't treated as local
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
    //
    // In local mode (baseUrl == null) return in-process implementations backed
    // by PrimitivesStore / BarrierCoordinator / QueueStore.
    //
    // In HTTP mode (baseUrl != null) register the primitive in the orchestrator's
    // in-memory store (so it is ready to serve HTTP requests), then return an
    // HTTP proxy.  The proxy carries the orchestrator URL so that when it is
    // captured in a lambda and shipped to a remote agent it still calls back to
    // the right place.

    @Override
    public DistributedCounter counter(String name, long initialValue) {
        primitives.getOrCreateCounter(name, initialValue); // ensure server-side entry exists
        if (baseUrl != null) return new HttpCounterProxy(baseUrl, name);
        return primitives.getOrCreateCounter(name, initialValue);
    }

    @Override
    public DistributedBoolean bool(String name, boolean initialValue) {
        primitives.getOrCreateBoolean(name, initialValue);
        if (baseUrl != null) return new HttpBooleanProxy(baseUrl, name);
        return primitives.getOrCreateBoolean(name, initialValue);
    }

    @Override
    public <T extends Serializable> DistributedReference<T> reference(String name, T initialValue) {
        primitives.getOrCreateReference(name, initialValue);
        if (baseUrl != null) return new HttpReferenceProxy<>(baseUrl, name);
        return primitives.getOrCreateReference(name, initialValue);
    }

    @Override
    public DistributedBarrier barrier(String name, int parties) {
        barriers.getOrCreate(name, parties); // ensure server-side barrier is registered
        if (baseUrl != null) return new HttpBarrierProxy(baseUrl, name, parties);
        return barriers.getOrCreate(name, parties);
    }

    @Override
    public <T extends Serializable> DistributedQueue<T> queue(String name) {
        if (baseUrl != null) return new HttpQueueProxy<>(baseUrl, name);
        return queues.getOrCreate(name);
    }

    // ── File transfer ──────────────────────────────────────────────────────────

    @Override
    public void upload(NodeId node, Path localPath, String remotePath) {
        if (Files.isDirectory(localPath)) {
            try (Stream<Path> stream = Files.walk(localPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    String rel = localPath.relativize(file).toString().replace(File.separatorChar, '/');
                    upload(node, file, remotePath + "/" + rel);
                });
            } catch (IOException e) {
                throw new RuntimeException("Upload directory to " + node + " failed: " + e.getMessage(), e);
            }
            return;
        }
        if (isLocalNode(node)) {
            try {
                String expanded = remotePath;
                if (expanded.startsWith("angela-root://")) {
                    String angelaRoot = System.getProperty("angela.rootDir",
                            System.getProperty("user.home") + "/.angela");
                    expanded = angelaRoot + "/" + expanded.substring("angela-root://".length());
                } else {
                    expanded = expanded.replaceFirst("^~(/|$)", System.getProperty("user.home") + "/");
                }
                Path target = Path.of(expanded);
                if (target.getParent() != null) Files.createDirectories(target.getParent());
                Files.copy(localPath, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new RuntimeException("Local upload failed: " + e.getMessage(), e);
            }
            return;
        }
        try {
            String url = "http://" + node.getHostname() + ":" + node.getPort()
                    + "/files?path=" + URLEncoder.encode(remotePath, StandardCharsets.UTF_8.name());
            OrchestratorHttp.postFile(url, localPath);
        } catch (IOException e) {
            throw new RuntimeException("Upload to " + node + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void download(NodeId node, String remotePath, Path localDest) {
        if (isLocalNode(node)) {
            try {
                String expanded = remotePath;
                if (expanded.startsWith("angela-root://")) {
                    String angelaRoot = System.getProperty("angela.rootDir",
                            System.getProperty("user.home") + "/.angela");
                    expanded = angelaRoot + "/" + expanded.substring("angela-root://".length());
                } else {
                    expanded = expanded.replaceFirst("^~(/|$)", System.getProperty("user.home") + "/");
                }
                Path source = Path.of(expanded);
                if (localDest.getParent() != null) Files.createDirectories(localDest.getParent());
                Files.copy(source, localDest, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new RuntimeException("Local download failed: " + e.getMessage(), e);
            }
            return;
        }
        try {
            String url = "http://" + node.getHostname() + ":" + node.getPort()
                    + "/files?path=" + URLEncoder.encode(remotePath, StandardCharsets.UTF_8.name());
            byte[] bytes = OrchestratorHttp.getBytes(url, 30_000);
            if (localDest.getParent() != null) Files.createDirectories(localDest.getParent());
            Files.write(localDest, bytes);
        } catch (IOException e) {
            throw new RuntimeException("Download from " + node + " failed: " + e.getMessage(), e);
        }
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return; // idempotent

        // Gracefully shut down connected remote agents before stopping the local server
        System.out.printf("[baton][%s][-] Closing fabric — shutting down remote agents%n", runId);
        for (NodeId node : registry.getConnectedNodes()) {
            if (!localNodes.contains(node) && !node.equals(localNodeId)) {
                try {
                    OrchestratorHttp.post(
                            "http://" + node.getHostname() + ":" + node.getPort() + "/shutdown");
                } catch (IOException ignored) {
                    // Agent may already be gone — that is fine
                }
            }
        }

        localPool.shutdownNow();
        registry.shutdown();
        dispatcher.failAll(new IllegalStateException("Fabric closed"));
        if (server != null) server.stop();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private boolean isLocalNode(NodeId node) {
        // Only nodes created via connectLocal() run in the local thread pool.
        // Remote agents that registered via /agent/register must NOT be treated
        // as local — they need HTTP dispatch.
        return localNodes.contains(node) || node.equals(localNodeId);
    }

    /**
     * Returns the best routable IP address for the orchestrator HTTP server.
     * Prefers a system property override ({@code baton.orchestrator.host}), then
     * the first non-loopback, non-link-local IPv4 address found on any interface,
     * falling back to {@code localhost}.
     */
    private static String resolveOrchestratorHost() {
        String override = System.getProperty("baton.orchestrator.host");
        if (override != null && !override.isEmpty()) return override;
        try {
            Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            while (ifaces.hasMoreElements()) {
                NetworkInterface iface = ifaces.nextElement();
                if (!iface.isUp() || iface.isLoopback()) continue;
                Enumeration<InetAddress> addrs = iface.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress addr = addrs.nextElement();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress() && !addr.isLinkLocalAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {}
        return "localhost";
    }

    private static NodeId buildLocalNodeId(int port) {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }
        int pid = (int) ProcessHandle.current().pid(); // ProcessHandle.pid() returns long
        return new NodeId("orchestrator", hostname, port, pid);
    }
}
