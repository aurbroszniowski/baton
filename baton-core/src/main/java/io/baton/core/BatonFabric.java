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

import io.baton.AgentEndpoint;
import io.baton.AgentLauncher;
import io.baton.DeploymentOptions;
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
import io.baton.transport.AgentFiles;
import io.baton.transport.HttpTransport;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
 * Owns orchestrator state, remote agent lifecycle, and optional in-process workers.
 */
public class BatonFabric implements Fabric {

    /** Short run ID for correlating log lines from the same orchestrator instance. */
    private final String             runId       = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private NodeId                   localNodeId;
    private final PrimitivesStore    primitives  = new PrimitivesStore();
    private final BarrierCoordinator barriers    = new BarrierCoordinator();
    private final QueueStore         queues      = new QueueStore();
    private final AgentRegistry      registry    = new AgentRegistry();
    private final JobDispatcher      dispatcher  = new JobDispatcher(registry::isAlive);
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
                new AgentDeadException("Agent is no longer connected: " + node)));
        BatonServer s   = null;
        String      url = null;
        if (orchestratorPort >= 0) { // -1 = local-only mode, no HTTP server
            try {
                s   = new BatonServer(primitives, barriers, queues, registry, dispatcher, logCollector, orchestratorPort);
                url = "http://" + resolveOrchestratorHost() + ":" + s.getPort();
            } catch (IOException e) {
                localPool.shutdownNow();
                registry.shutdown();
                throw new IllegalStateException("Could not start orchestrator HTTP server", e);
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

    // Node lifecycle 

    @Override
    public NodeId getLocalNodeId() { return localNodeId; }

    @Override
    public NodeId deployAndConnect(String hostname, SshConfig ssh) {
        return deploy(hostname, ssh, null);
    }

    @Override
    public NodeId deployAndConnect(String hostname, SshConfig ssh, DeploymentOptions options) {
        return deploy(hostname, ssh, java.util.Objects.requireNonNull(options, "options"));
    }

    private NodeId deploy(String hostname, SshConfig ssh, DeploymentOptions options) {
        if (closed.get()) throw new IllegalStateException("Fabric closed");
        if (baseUrl == null) {
            throw new IllegalStateException("Fabric must be started with a port to support deployAndConnect");
        }
        AgentLauncher launcher = ServiceLoader.load(AgentLauncher.class)
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException(
                        "No AgentLauncher found on classpath — add baton-core as a dependency"));
        try {
            long previousGeneration = registry.registrationGeneration();
            AgentEndpoint endpoint = options == null ? launcher.start(hostname, ssh, baseUrl)
                    : launcher.start(hostname, ssh, baseUrl, options);
            return registry.awaitRegistration(endpoint.getHostname(), endpoint.getPort(), 30_000, previousGeneration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for agent registration", e);
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deploy agent on " + hostname, e);
        }
    }

    @Override
    public NodeId connectLocal() {
        if (closed.get()) throw new IllegalStateException("Fabric closed");
        // Spawn a local "node" in the same JVM (useful for unit tests)
        NodeId node = new NodeId("local-" + System.nanoTime(), localNodeId.getHostname(), 0, (int) ProcessHandle.current().pid());
        localNodes.add(node); // track separately so remote agents aren't treated as local
        return node;
    }

    @Override
    public Collection<NodeId> getConnectedNodes() {
        Collection<NodeId> nodes = new ArrayList<>(localNodes);
        nodes.addAll(registry.getConnectedNodes());
        return Collections.unmodifiableCollection(nodes);
    }

    @Override
    public void disconnect(NodeId node) {
        disconnect(node, HttpTransport.DEFAULT_TIMEOUT_MS);
    }

    private void disconnect(NodeId node, int timeoutMs) {
        if (localNodes.remove(node)) return;
        try {
            if (timeoutMs > 0) HttpTransport.post("http://" + node.getHostname() + ":" + node.getPort() + "/shutdown",
                    new byte[0], "application/octet-stream", timeoutMs);
        } catch (IOException ignored) {
            // An unreachable agent still needs to be removed and its jobs failed.
        } finally {
            registry.remove(node);
            dispatcher.failAgent(node, new AgentDeadException("Agent disconnected: " + node));
        }
    }

    // Remote execution 

    @Override
    public Future<Void> executeAsync(NodeId node, RemoteRunnable job) {
        if (closed.get()) return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("Fabric closed"));
        if (isLocalNode(node)) {
            return localPool.submit(() -> { job.run(); return null; });
        }
        return dispatcher.dispatchRunnable(node, job);
    }

    @Override
    public <R> Future<R> executeAsync(NodeId node, RemoteCallable<R> job) {
        if (closed.get()) return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("Fabric closed"));
        if (isLocalNode(node)) {
            return localPool.submit(job::call);
        }
        return dispatcher.dispatchCallable(node, job);
    }

    // Distributed primitives 
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
        DistributedCounter counter = primitives.getOrCreateCounter(name, initialValue);
        return baseUrl == null ? counter : new HttpCounterProxy(baseUrl, name);
    }

    @Override
    public DistributedBoolean bool(String name, boolean initialValue) {
        DistributedBoolean flag = primitives.getOrCreateBoolean(name, initialValue);
        return baseUrl == null ? flag : new HttpBooleanProxy(baseUrl, name);
    }

    @Override
    public <T extends Serializable> DistributedReference<T> reference(String name, T initialValue) {
        DistributedReference<T> reference = primitives.getOrCreateReference(name, initialValue);
        return baseUrl == null ? reference : new HttpReferenceProxy<>(baseUrl, name);
    }

    @Override
    public DistributedBarrier barrier(String name, int parties) {
        DistributedBarrier barrier = barriers.getOrCreate(name, parties);
        return baseUrl == null ? barrier : new HttpBarrierProxy(baseUrl, name, parties);
    }

    @Override
    public <T extends Serializable> DistributedQueue<T> queue(String name) {
        if (baseUrl != null) return new HttpQueueProxy<>(baseUrl, name);
        return queues.getOrCreate(name);
    }

    // File transfer 

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
                AgentFiles.copy(localPath, AgentFiles.resolve(remotePath));
            } catch (IOException e) {
                throw new RuntimeException("Local upload failed: " + e.getMessage(), e);
            }
            return;
        }
        try {
            String url = "http://" + node.getHostname() + ":" + node.getPort()
                    + "/files?path=" + HttpTransport.encode(remotePath);
            HttpTransport.postFile(url, localPath);
        } catch (IOException e) {
            throw new RuntimeException("Upload to " + node + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void download(NodeId node, String remotePath, Path localDest) {
        if (isLocalNode(node)) {
            try {
                AgentFiles.copy(AgentFiles.resolve(remotePath), localDest);
            } catch (IOException e) {
                throw new RuntimeException("Local download failed: " + e.getMessage(), e);
            }
            return;
        }
        try {
            String url = "http://" + node.getHostname() + ":" + node.getPort()
                    + "/files?path=" + HttpTransport.encode(remotePath);
            HttpTransport.download(url, localDest);
        } catch (IOException e) {
            throw new RuntimeException("Download from " + node + " failed: " + e.getMessage(), e);
        }
    }

    // Lifecycle 

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return; // idempotent
        dispatcher.failAll(new IllegalStateException("Fabric closed"));

        // Gracefully shut down connected remote agents before stopping the local server
        System.out.printf("[baton][%s][-] Closing fabric — shutting down remote agents%n", runId);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        for (NodeId node : registry.getConnectedNodes()) {
            int remaining = (int) Math.max(0, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
            disconnect(node, Math.min(500, remaining));
        }

        localNodes.clear();
        localPool.shutdownNow();
        registry.shutdown();
        if (server != null) server.stop();
    }

    // Helpers 

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
