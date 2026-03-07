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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2 — HTTP transport integration tests.
 *
 * <p>Two "sides" communicate over a real HTTP server started in-process:
 * <ul>
 *   <li>Side A: uses the {@link BatonFabric} API in HTTP mode (returns HTTP proxies).
 *   <li>Side B: creates {@code Http*Proxy} instances directly, simulating a remote agent
 *       that received the proxy objects via a serialized lambda.
 * </ul>
 * All state lives in the orchestrator's in-memory store; all operations cross
 * the HTTP boundary — even when orchestrator and client are in the same JVM.
 */
class HttpTransportIT {

    private BatonFabric fabric;
    private String      baseUrl;

    @BeforeEach
    void setUp() {
        fabric  = new BatonFabric(0); // port 0 → OS picks a free port
        baseUrl = "http://localhost:" + fabric.getOrchestratorPort();
    }

    @AfterEach
    void tearDown() {
        fabric.close();
    }

    // ── DistributedCounter ─────────────────────────────────────────────────────

    @Test
    void httpCounter_twoSidesShareState() {
        // Side A: create via Fabric (HTTP mode → HttpCounterProxy)
        DistributedCounter a = fabric.counter("c", 10L);

        // Side B: direct proxy — simulates a remote agent that received the proxy
        HttpCounterProxy b = new HttpCounterProxy(baseUrl, "c");

        assertEquals(10L, a.get());
        assertEquals(10L, b.get());

        a.incrementAndGet();
        assertEquals(11L, b.get());   // side B sees the change

        b.incrementAndGet();
        assertEquals(12L, a.get());   // side A sees the change
    }

    @Test
    void httpCounter_compareAndSet() {
        DistributedCounter c = fabric.counter("cas-c", 5L);
        assertTrue(c.compareAndSet(5L, 99L));
        assertFalse(c.compareAndSet(5L, 0L)); // stale expect
        assertEquals(99L, c.get());
    }

    @Test
    void httpCounter_getAndSet() {
        DistributedCounter c = fabric.counter("gas-c", 7L);
        assertEquals(7L, c.getAndSet(42L));
        assertEquals(42L, c.get());
    }

    // ── DistributedBoolean ─────────────────────────────────────────────────────

    @Test
    void httpBoolean_twoSidesShareState() {
        DistributedBoolean a = fabric.bool("b", false);
        HttpBooleanProxy   b = new HttpBooleanProxy(baseUrl, "b");

        assertFalse(a.get());
        assertFalse(b.get());

        a.set(true);
        assertTrue(b.get());

        b.set(false);
        assertFalse(a.get());
    }

    @Test
    void httpBoolean_compareAndSet() {
        DistributedBoolean b = fabric.bool("cas-b", false);
        assertTrue(b.compareAndSet(false, true));
        assertFalse(b.compareAndSet(false, true)); // already true
        assertTrue(b.get());
    }

    // ── DistributedReference ───────────────────────────────────────────────────

    @Test
    void httpReference_twoSidesShareState() {
        DistributedReference<String> a = fabric.reference("r", "initial");
        HttpReferenceProxy<String>   b = new HttpReferenceProxy<>(baseUrl, "r");

        assertEquals("initial", a.get());
        assertEquals("initial", b.get());

        a.set("updated-by-a");
        assertEquals("updated-by-a", b.get());

        b.set("updated-by-b");
        assertEquals("updated-by-b", a.get());
    }

    @Test
    void httpReference_compareAndSet() {
        DistributedReference<String> r = fabric.reference("cas-r", "old");
        assertTrue(r.compareAndSet("old", "new"));
        assertFalse(r.compareAndSet("old", "other")); // stale expect
        assertEquals("new", r.get());
    }

    // ── DistributedQueue ───────────────────────────────────────────────────────

    @Test
    void httpQueue_putAndTake() throws Exception {
        DistributedQueue<String> a = fabric.queue("q");
        HttpQueueProxy<String>   b = new HttpQueueProxy<>(baseUrl, "q");

        a.put("hello");
        assertEquals("hello", b.take());

        b.put("world");
        assertEquals("world", a.take());
    }

    @Test
    void httpQueue_pollTimeout_returnsNullWhenEmpty() throws Exception {
        DistributedQueue<String> q = fabric.queue("q-empty");
        assertNull(q.poll(50, TimeUnit.MILLISECONDS));
    }

    @Test
    void httpQueue_fifoOrder() throws Exception {
        DistributedQueue<String> q = fabric.queue("q-fifo");
        q.put("first");
        q.put("second");
        q.put("third");
        assertEquals("first",  q.take());
        assertEquals("second", q.take());
        assertEquals("third",  q.take());
    }

    // ── DistributedBarrier ─────────────────────────────────────────────────────

    @Test
    void httpBarrier_twoThreadsReleaseTogether() throws Exception {
        // Register the barrier server-side first (so the HTTP handler knows the party count)
        fabric.barrier("bar", 2);

        HttpBarrierProxy b1 = new HttpBarrierProxy(baseUrl, "bar", 2);
        HttpBarrierProxy b2 = new HttpBarrierProxy(baseUrl, "bar", 2);

        int[] indices = new int[2];
        Thread t1 = new Thread(() -> {
            try { indices[0] = b1.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        Thread t2 = new Thread(() -> {
            try { indices[1] = b2.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });

        t1.start();
        t2.start();
        t1.join(5_000);
        t2.join(5_000);

        assertFalse(t1.isAlive(), "t1 should have completed");
        assertFalse(t2.isAlive(), "t2 should have completed");

        // One thread gets index 0 (first to arrive), the other gets 1 (last)
        assertEquals(1, indices[0] + indices[1]);
    }

    @Test
    void httpBarrier_timeoutWhenOnlyOnePartyArrives() {
        fabric.barrier("bar-timeout", 2); // needs 2 parties
        HttpBarrierProxy b = new HttpBarrierProxy(baseUrl, "bar-timeout", 2);
        assertThrows(Exception.class, () -> b.await(150, TimeUnit.MILLISECONDS));
    }

    @Test
    void httpBarrier_timeoutMessage_includesArrivedCount() {
        // Phase 6: the TimeoutException should carry "X/Y arrived" detail
        fabric.barrier("bar-detail", 2);
        HttpBarrierProxy b = new HttpBarrierProxy(baseUrl, "bar-detail", 2);
        TimeoutException ex = assertThrows(TimeoutException.class,
                () -> b.await(200, TimeUnit.MILLISECONDS));
        assertNotNull(ex.getMessage(), "TimeoutException must have a message");
        assertTrue(ex.getMessage().contains("/2"),
                "Expected 'X/2 arrived' in: " + ex.getMessage());
    }
}
