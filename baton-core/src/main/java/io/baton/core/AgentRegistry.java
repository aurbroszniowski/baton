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

import io.baton.NodeId;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Tracks registered agents and detects dead ones via heartbeat timeouts.
 *
 * <p>Each agent sends a heartbeat every 5 s.  After 3 consecutive misses
 * (15 s grace period) the agent is marked dead and all pending jobs for it
 * are failed with an {@link AgentDeadException}.
 */
class AgentRegistry {

    static final long HEARTBEAT_INTERVAL_MS = 5_000;
    static final int  MISSED_HEARTBEATS_DEAD = 3;
    static final long GRACE_PERIOD_MS = HEARTBEAT_INTERVAL_MS * MISSED_HEARTBEATS_DEAD;

    private final ConcurrentHashMap<NodeId, AgentEntry> agents = new ConcurrentHashMap<>();

    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "baton-heartbeat-monitor");
        t.setDaemon(true);
        return t;
    });

    /** Called when an agent is declared dead; wired from {@link BatonFabric}. */
    private volatile Consumer<NodeId> deathListener = node -> {};
    /** Correlation prefix for log lines; set by {@link BatonFabric}. */
    private volatile String runId = "-";
    private boolean closed;
    private long registrationGeneration;

    AgentRegistry() {
        monitor.scheduleAtFixedRate(
                this::checkHeartbeats,
                GRACE_PERIOD_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    void setDeathListener(Consumer<NodeId> listener) { this.deathListener = listener; }
    void setRunId(String runId)                      { this.runId = runId; }

    synchronized void register(NodeId nodeId) {
        if (closed) throw new IllegalStateException("Fabric closed");
        agents.forEach((node, entry) -> {
            if (!node.equals(nodeId) && node.getHostname().equals(nodeId.getHostname())
                    && node.getPort() == nodeId.getPort() && agents.remove(node, entry)) {
                deathListener.accept(node);
            }
        });
        agents.put(nodeId, new AgentEntry(nodeId, ++registrationGeneration));
        notifyAll();
    }

    boolean heartbeat(NodeId nodeId) {
        AgentEntry entry = agents.get(nodeId);
        if (entry != null) entry.lastHeartbeatMs = System.currentTimeMillis();
        return entry != null;
    }

    boolean isAlive(NodeId nodeId) {
        AgentEntry entry = agents.get(nodeId);
        return entry != null;
    }

    Collection<NodeId> getConnectedNodes() {
        return Collections.unmodifiableCollection(new java.util.ArrayList<>(agents.keySet()));
    }

    synchronized long registrationGeneration() { return registrationGeneration; }

    synchronized NodeId awaitRegistration(String hostname, int port, long timeoutMs)
            throws InterruptedException, TimeoutException {
        return awaitRegistration(hostname, port, timeoutMs, 0);
    }

    synchronized NodeId awaitRegistration(String hostname, int port, long timeoutMs, long afterGeneration)
            throws InterruptedException, TimeoutException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (true) {
            if (closed) throw new IllegalStateException("Fabric closed");
            for (AgentEntry entry : agents.values()) {
                NodeId node = entry.nodeId;
                if (node.getHostname().equals(hostname) && node.getPort() == port
                        && entry.generation > afterGeneration) return node;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Agent did not register: " + hostname + ":" + port);
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
    }

    void remove(NodeId nodeId) {
        agents.remove(nodeId);
    }

    synchronized void shutdown() {
        closed = true;
        agents.clear();
        monitor.shutdownNow();
        notifyAll();
    }

    private void checkHeartbeats() {
        checkHeartbeats(System.currentTimeMillis());
    }

    void checkHeartbeats(long now) {
        for (AgentEntry entry : agents.values()) {
            if ((now - entry.lastHeartbeatMs) > GRACE_PERIOD_MS && agents.remove(entry.nodeId, entry)) {
                System.err.printf("[baton][%s][-] Agent %s missed heartbeat — marking dead%n",
                        runId, entry.nodeId);
                deathListener.accept(entry.nodeId);
            }
        }
    }

    private static class AgentEntry {
        final NodeId nodeId;
        final long generation;
        volatile long    lastHeartbeatMs = System.currentTimeMillis();

        AgentEntry(NodeId nodeId, long generation) {
            this.nodeId = nodeId;
            this.generation = generation;
        }
    }
}
