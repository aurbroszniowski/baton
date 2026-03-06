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
package io.baton.angela;

import io.baton.DistributedCounter;
import io.baton.Fabric;
import io.baton.FabricFactory;
import io.baton.NodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.terracotta.angela.agent.com.AgentGroup;
import org.terracotta.angela.agent.com.AgentID;
import org.terracotta.angela.common.cluster.AtomicBoolean;
import org.terracotta.angela.common.cluster.AtomicCounter;
import org.terracotta.angela.common.cluster.AtomicReference;
import org.terracotta.angela.common.cluster.Barrier;
import org.terracotta.angela.common.cluster.Cluster;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 4 — Angela adapter unit tests in local (in-process) mode.
 *
 * <p>These tests mirror the spirit of Angela's {@code ClientIT} in local mode:
 * counter/boolean/reference/barrier primitives via {@link BatonClusterPrimitives},
 * remote execution via {@link BatonExecutor}, and the {@code getGroup()} /
 * {@code getCluster()} adapter methods.
 *
 * <p>All Angela {@code ClientIT} tests that are NOT guarded by
 * {@code assumeFalse("Cannot run without Ignite", agentID.isLocal())} are
 * effectively covered here (the rest require Ignite remoting and are out of
 * scope for Phase 4 local mode).
 */
class AdapterLocalIT {

    private Fabric                fabric;
    private BatonClusterPrimitives primitives;
    private BatonExecutor          executor;

    @BeforeEach
    void setUp() {
        fabric     = FabricFactory.create();   // local-only (no HTTP server)
        primitives = new BatonClusterPrimitives(fabric);
        executor   = new BatonExecutor(fabric, UUID.randomUUID());
    }

    @AfterEach
    void tearDown() {
        fabric.close();
    }

    // ── AtomicCounter ─────────────────────────────────────────────────────────

    @Test
    void atomicCounter_initialValue() {
        AtomicCounter c = primitives.atomicCounter("c", 42L);
        assertEquals(42L, c.get());
    }

    @Test
    void atomicCounter_incrementAndGet() {
        AtomicCounter c = primitives.atomicCounter("c", 0L);
        assertEquals(1L, c.incrementAndGet());
        assertEquals(2L, c.incrementAndGet());
    }

    @Test
    void atomicCounter_getAndIncrement() {
        AtomicCounter c = primitives.atomicCounter("c", 10L);
        assertEquals(10L, c.getAndIncrement());
        assertEquals(11L, c.get());
    }

    @Test
    void atomicCounter_getAndSet() {
        AtomicCounter c = primitives.atomicCounter("c", 5L);
        assertEquals(5L, c.getAndSet(99L));
        assertEquals(99L, c.get());
    }

    @Test
    void atomicCounter_compareAndSet() {
        AtomicCounter c = primitives.atomicCounter("c", 1L);
        assertTrue(c.compareAndSet(1L, 2L));
        assertFalse(c.compareAndSet(1L, 3L)); // stale expect
        assertEquals(2L, c.get());
    }

    @Test
    void atomicCounter_sameNameSharedState() {
        AtomicCounter c1 = primitives.atomicCounter("shared", 0L);
        AtomicCounter c2 = primitives.atomicCounter("shared", 0L);
        c1.incrementAndGet();
        assertEquals(1L, c2.get());
    }

    // ── AtomicBoolean ─────────────────────────────────────────────────────────

    @Test
    void atomicBoolean_initialValue() {
        assertFalse(primitives.atomicBoolean("b-false", false).get());
        assertTrue(primitives.atomicBoolean("b-true", true).get());
    }

    @Test
    void atomicBoolean_setAndGet() {
        AtomicBoolean b = primitives.atomicBoolean("b", false);
        b.set(true);
        assertTrue(b.get());
    }

    @Test
    void atomicBoolean_getAndSet() {
        AtomicBoolean b = primitives.atomicBoolean("b", true);
        assertTrue(b.getAndSet(false));
        assertFalse(b.get());
    }

    @Test
    void atomicBoolean_compareAndSet() {
        AtomicBoolean b = primitives.atomicBoolean("b", false);
        assertTrue(b.compareAndSet(false, true));
        assertFalse(b.compareAndSet(false, true)); // already true
        assertTrue(b.get());
    }

    // ── AtomicReference ───────────────────────────────────────────────────────

    @Test
    void atomicReference_initialValue() {
        AtomicReference<String> r = primitives.atomicReference("r", "hello");
        assertEquals("hello", r.get());
    }

    @Test
    void atomicReference_set() {
        AtomicReference<String> r = primitives.atomicReference("r", "a");
        r.set("b");
        assertEquals("b", r.get());
    }

    @Test
    void atomicReference_compareAndSet() {
        AtomicReference<String> r = primitives.atomicReference("r", "old");
        assertTrue(r.compareAndSet("old", "new"));
        assertFalse(r.compareAndSet("old", "other")); // stale
        assertEquals("new", r.get());
    }

    // ── Barrier ───────────────────────────────────────────────────────────────

    @Test
    void barrier_twoThreadsReleaseTogether() throws Exception {
        Barrier b = primitives.barrier("bar", 2);
        int[] indices = new int[2];

        Thread t1 = new Thread(() -> { indices[0] = b.await(); });
        Thread t2 = new Thread(() -> { indices[1] = b.await(); });
        t1.start();
        t2.start();
        t1.join(5_000);
        t2.join(5_000);

        assertFalse(t1.isAlive(), "t1 should have completed");
        assertFalse(t2.isAlive(), "t2 should have completed");
        assertEquals(1, indices[0] + indices[1]);
    }

    @Test
    void barrier_timeout() {
        Barrier b = primitives.barrier("bar-timeout", 2); // needs 2, only 1 arrives
        assertThrows(TimeoutException.class,
                () -> b.await(100, TimeUnit.MILLISECONDS));
    }

    // ── BatonExecutor — executeAsync ──────────────────────────────────────────

    @Test
    void executeAsync_callable_returnsValue() throws Exception {
        NodeId  node    = fabric.connectLocal();
        AgentID agentId = BatonExecutor.toAngela(node);

        Future<Integer> f = executor.executeAsync(agentId, () -> 42);
        assertEquals(42, f.get(5, TimeUnit.SECONDS));
    }

    @Test
    void executeAsync_callable_capturesValue() throws Exception {
        NodeId  node    = fabric.connectLocal();
        AgentID agentId = BatonExecutor.toAngela(node);
        String  msg     = "hello";

        Future<String> f = executor.executeAsync(agentId, () -> msg.toUpperCase());
        assertEquals("HELLO", f.get(5, TimeUnit.SECONDS));
    }

    @Test
    void executeAsync_runnable_mutatesCounter() throws Exception {
        NodeId  node    = fabric.connectLocal();
        AgentID agentId = BatonExecutor.toAngela(node);

        // In local mode fabric.counter returns an in-process LocalCounter
        DistributedCounter counter = fabric.counter("exec-c", 0L);
        Future<Void> f = executor.executeAsync(agentId, () -> { counter.incrementAndGet(); });
        f.get(5, TimeUnit.SECONDS);
        assertEquals(1L, counter.get());
    }

    @Test
    void executeAsync_exceptionPropagates() {
        NodeId  node    = fabric.connectLocal();
        AgentID agentId = BatonExecutor.toAngela(node);

        Future<Void> f = executor.executeAsync(agentId,
                () -> { throw new IllegalStateException("boom"); });
        assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
    }

    // ── BatonExecutor — getGroup / getCluster ─────────────────────────────────

    @Test
    void getGroup_containsLocalAgent() {
        AgentGroup group = executor.getGroup();
        assertNotNull(group);
        assertTrue(group.getAllAgents().contains(executor.getLocalAgentID()),
                "Group must contain the local orchestrator agent");
    }

    @Test
    void getGroup_includesConnectedNodes() {
        NodeId  node    = fabric.connectLocal();
        AgentID agentId = BatonExecutor.toAngela(node);

        AgentGroup group = executor.getGroup();
        assertTrue(group.getAllAgents().contains(agentId),
                "Group must include nodes registered via connectLocal()");
    }

    @Test
    void getCluster_primitivesDelegateToFabric() {
        Cluster cluster = executor.getCluster();
        assertNotNull(cluster);

        // Counter set through cluster is visible through direct primitives
        cluster.atomicCounter("cluster-c", 0L).incrementAndGet();
        assertEquals(1L, primitives.atomicCounter("cluster-c", 0L).get());
    }

    @Test
    void getCluster_noClientId_returnsNullClientId() {
        Cluster cluster = executor.getCluster();
        assertNull(cluster.getClientId());
    }

    // ── ID conversions ────────────────────────────────────────────────────────

    @Test
    void toAngela_roundTrip() {
        NodeId  original  = new NodeId("agent-1", "host-a", 9000, 12345);
        AgentID angela    = BatonExecutor.toAngela(original);
        NodeId  roundTrip = BatonExecutor.toNodeId(angela);

        assertEquals(original.getName(),     roundTrip.getName());
        assertEquals(original.getHostname(), roundTrip.getHostname());
        assertEquals(original.getPort(),     roundTrip.getPort());
        assertEquals(original.getPid(),      roundTrip.getPid());
    }
}
