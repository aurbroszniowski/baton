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
import io.baton.FabricFactory;
import io.baton.NodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 — standalone unit tests of the Fabric API in local (in-process) mode.
 * No Angela, no HTTP, no SSH.
 */
class FabricTest {

    private Fabric fabric;

    @BeforeEach
    void setUp() {
        fabric = FabricFactory.create(); // local-only, no HTTP server
    }

    @AfterEach
    void tearDown() {
        fabric.close();
    }

    // ── DistributedCounter ─────────────────────────────────────────────────────

    @Test
    void counter_initialValue() {
        DistributedCounter c = fabric.counter("c", 42L);
        assertEquals(42L, c.get());
    }

    @Test
    void counter_incrementAndGet() {
        DistributedCounter c = fabric.counter("c", 0L);
        assertEquals(1L, c.incrementAndGet());
        assertEquals(2L, c.incrementAndGet());
    }

    @Test
    void counter_getAndIncrement() {
        DistributedCounter c = fabric.counter("c", 10L);
        assertEquals(10L, c.getAndIncrement());
        assertEquals(11L, c.get());
    }

    @Test
    void counter_getAndSet() {
        DistributedCounter c = fabric.counter("c", 5L);
        assertEquals(5L, c.getAndSet(99L));
        assertEquals(99L, c.get());
    }

    @Test
    void counter_compareAndSet_success() {
        DistributedCounter c = fabric.counter("c", 1L);
        assertTrue(c.compareAndSet(1L, 2L));
        assertEquals(2L, c.get());
    }

    @Test
    void counter_compareAndSet_failure() {
        DistributedCounter c = fabric.counter("c", 1L);
        assertFalse(c.compareAndSet(99L, 2L));
        assertEquals(1L, c.get());
    }

    @Test
    void counter_sameNameReturnsSameInstance() {
        DistributedCounter c1 = fabric.counter("shared", 0L);
        DistributedCounter c2 = fabric.counter("shared", 0L);
        c1.incrementAndGet();
        assertEquals(1L, c2.get());
    }

    // ── DistributedBoolean ─────────────────────────────────────────────────────

    @Test
    void bool_initialValue() {
        assertFalse(fabric.bool("b", false).get());
        assertTrue(fabric.bool("b2", true).get());
    }

    @Test
    void bool_set() {
        DistributedBoolean b = fabric.bool("b", false);
        b.set(true);
        assertTrue(b.get());
    }

    @Test
    void bool_getAndSet() {
        DistributedBoolean b = fabric.bool("b", true);
        assertTrue(b.getAndSet(false));
        assertFalse(b.get());
    }

    @Test
    void bool_compareAndSet() {
        DistributedBoolean b = fabric.bool("b", false);
        assertTrue(b.compareAndSet(false, true));
        assertTrue(b.get());
        assertFalse(b.compareAndSet(false, true)); // already true
    }

    // ── DistributedReference ───────────────────────────────────────────────────

    @Test
    void reference_initialValue() {
        DistributedReference<String> r = fabric.reference("r", "hello");
        assertEquals("hello", r.get());
    }

    @Test
    void reference_set() {
        DistributedReference<String> r = fabric.reference("r", "a");
        r.set("b");
        assertEquals("b", r.get());
    }

    @Test
    void reference_compareAndSet() {
        DistributedReference<String> r = fabric.reference("r", "old");
        assertTrue(r.compareAndSet("old", "new"));
        assertEquals("new", r.get());
        assertFalse(r.compareAndSet("old", "other")); // stale expect
        assertEquals("new", r.get());
    }

    // ── DistributedQueue ───────────────────────────────────────────────────────

    @Test
    void queue_putAndTake() throws Exception {
        DistributedQueue<String> q = fabric.queue("q");
        q.put("hello");
        assertEquals("hello", q.take());
    }

    @Test
    void queue_pollTimeout_returnsNullWhenEmpty() throws Exception {
        DistributedQueue<String> q = fabric.queue("q");
        assertNull(q.poll(50, TimeUnit.MILLISECONDS));
    }

    @Test
    void queue_fifoOrder() throws Exception {
        DistributedQueue<String> q = fabric.queue("q");
        q.put("first");
        q.put("second");
        q.put("third");
        assertEquals("first", q.take());
        assertEquals("second", q.take());
        assertEquals("third", q.take());
    }

    // ── DistributedBarrier ─────────────────────────────────────────────────────

    @Test
    void barrier_releasesBothParties() throws Exception {
        DistributedBarrier barrier = fabric.barrier("b", 2);
        NodeId n1 = fabric.connectLocal();
        NodeId n2 = fabric.connectLocal();

        int[] indices = new int[2]; // effectively final, safe: one written per slot before get()

        Future<Void> f1 = fabric.executeAsync(n1, () -> { indices[0] = barrier.await(); });
        Future<Void> f2 = fabric.executeAsync(n2, () -> { indices[1] = barrier.await(); });

        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);

        // arrival indices must be 0 and 1 (in either order)
        assertEquals(1, indices[0] + indices[1]);
    }

    @Test
    void barrier_timeout_throwsOnTimeout() {
        DistributedBarrier barrier = fabric.barrier("b", 2); // needs 2 parties, only 1 arrives
        assertThrows(Exception.class, () -> barrier.await(100, TimeUnit.MILLISECONDS));
    }

    // ── executeAsync ───────────────────────────────────────────────────────────

    @Test
    void executeAsync_runnable_runsOnLocalNode() throws Exception {
        NodeId local = fabric.connectLocal();
        DistributedCounter counter = fabric.counter("exec", 0L);

        Future<Void> f = fabric.executeAsync(local, () -> { counter.incrementAndGet(); });
        f.get(5, TimeUnit.SECONDS);

        assertEquals(1L, counter.get());
    }

    @Test
    void executeAsync_callable_returnsValue() throws Exception {
        NodeId local = fabric.connectLocal();

        Future<String> f = fabric.executeAsync(local, () -> "hello from lambda");
        assertEquals("hello from lambda", f.get(5, TimeUnit.SECONDS));
    }

    @Test
    void executeAsync_callable_capturesPrimitive() throws Exception {
        NodeId local = fabric.connectLocal();
        DistributedCounter counter = fabric.counter("cap", 7L);

        Future<Long> f = fabric.executeAsync(local, counter::get);
        assertEquals(7L, f.get(5, TimeUnit.SECONDS));
    }

    @Test
    void executeAsync_exceptionPropagates() {
        NodeId local = fabric.connectLocal();
        Future<Void> f = fabric.executeAsync(local, () -> { throw new IllegalStateException("boom"); });
        assertThrows(Exception.class, () -> f.get(5, TimeUnit.SECONDS));
    }

    // ── Node lifecycle ─────────────────────────────────────────────────────────

    @Test
    void connectLocal_appearsInConnectedNodes() {
        NodeId node = fabric.connectLocal();
        assertTrue(fabric.getConnectedNodes().contains(node));
    }

    @Test
    void disconnect_removesFromConnectedNodes() {
        NodeId node = fabric.connectLocal();
        fabric.disconnect(node);
        assertFalse(fabric.getConnectedNodes().contains(node));
    }
}
