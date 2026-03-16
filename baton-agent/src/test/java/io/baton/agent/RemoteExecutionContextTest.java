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
package io.baton.agent;

import io.baton.RemoteExecutionContext;
import io.baton.core.BatonFabric;
import io.baton.core.LogRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RemoteExecutionContext}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Context is visible within the thread that set it</li>
 *   <li>Context is cleared after {@link RemoteExecutionContext#clear()}</li>
 *   <li>Concurrent threads maintain independent contexts</li>
 *   <li>Null accessors outside a job context</li>
 * </ul>
 */
class RemoteExecutionContextTest {

    @AfterEach
    void cleanup() {
        // Ensure the test thread's context is always cleared
        RemoteExecutionContext.clear();
    }

    // ── Basic set / get / clear ───────────────────────────────────────────────

    @Test
    void get_returnsNullBeforeSet() {
        assertNull(RemoteExecutionContext.nodeId());
        assertNull(RemoteExecutionContext.jobId());
    }

    @Test
    void set_valuesAreReadableOnSameThread() {
        RemoteExecutionContext.set("node-42", "job-abc");

        assertEquals("node-42", RemoteExecutionContext.nodeId());
        assertEquals("job-abc", RemoteExecutionContext.jobId());
    }

    @Test
    void clear_removesContext() {
        RemoteExecutionContext.set("node-1", "job-1");
        RemoteExecutionContext.clear();

        assertNull(RemoteExecutionContext.nodeId());
        assertNull(RemoteExecutionContext.jobId());
    }

    @Test
    void clear_isIdempotent() {
        RemoteExecutionContext.clear(); // no context was set — must not throw
        RemoteExecutionContext.clear();

        assertNull(RemoteExecutionContext.jobId());
    }

    @Test
    void set_overwritesPreviousContext() {
        RemoteExecutionContext.set("node-1", "job-1");
        RemoteExecutionContext.set("node-2", "job-2");

        assertEquals("node-2", RemoteExecutionContext.nodeId());
        assertEquals("job-2",  RemoteExecutionContext.jobId());
    }

    // ── Thread isolation ──────────────────────────────────────────────────────

    @Test
    void context_isNotVisibleOnOtherThreads() throws InterruptedException {
        RemoteExecutionContext.set("node-main", "job-main");

        AtomicReference<String> otherJobId = new AtomicReference<>("NOT-SET");
        Thread t = new Thread(() -> otherJobId.set(RemoteExecutionContext.jobId()));
        t.start();
        t.join();

        assertNull(otherJobId.get(), "Other thread must not see the main thread's context");
    }

    @Test
    void context_concurrentJobsHaveIndependentContexts() throws Exception {
        int nThreads = 8;
        CountDownLatch allSet    = new CountDownLatch(nThreads);
        CountDownLatch allRead   = new CountDownLatch(1);
        List<String>   readBack  = new CopyOnWriteArrayList<>();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final String myJob = "concurrent-job-" + i;
            Thread t = new Thread(() -> {
                RemoteExecutionContext.set("node-x", myJob);
                allSet.countDown();
                try { allRead.await(); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                readBack.add(RemoteExecutionContext.jobId());
                RemoteExecutionContext.clear();
            }, "ctx-thread-" + myJob);
            threads.add(t);
        }
        threads.forEach(Thread::start);
        allSet.await();    // all threads have set their context
        allRead.countDown(); // release all at once so they read while others are active
        for (Thread t : threads) t.join();

        // Every thread must have read back its own job ID — no cross-thread leakage
        assertEquals(nThreads, readBack.size());
        for (int i = 0; i < nThreads; i++) {
            assertTrue(readBack.contains("concurrent-job-" + i),
                    "Expected 'concurrent-job-" + i + "' in " + readBack);
        }
        // Main thread context is unchanged
        assertEquals("node-main-not-set", RemoteExecutionContext.nodeId() == null
                ? "node-main-not-set" : RemoteExecutionContext.nodeId());
    }

    // ── Simulate JobRunner lifecycle ──────────────────────────────────────────

    @Test
    void simulatedJobRunner_contextSetBeforeAndClearedAfter() {
        assertNull(RemoteExecutionContext.jobId(), "No context before job");

        RemoteExecutionContext.set("agent@host:8700#1", "aabbccdd");
        try {
            // Simulate job body reading context
            assertEquals("aabbccdd", RemoteExecutionContext.jobId());
            assertEquals("agent@host:8700#1", RemoteExecutionContext.nodeId());
        } finally {
            RemoteExecutionContext.clear();
        }

        assertNull(RemoteExecutionContext.jobId(), "Context must be cleared after job");
        assertNull(RemoteExecutionContext.nodeId(), "Context must be cleared after job");
    }

    @Test
    void simulatedJobRunner_contextClearedEvenOnException() {
        RemoteExecutionContext.set("agent", "failing-job");
        try {
            assertThrows(RuntimeException.class, () -> {
                try {
                    throw new RuntimeException("job failed");
                } finally {
                    RemoteExecutionContext.clear();
                }
            });
        } catch (Exception ignored) {}

        assertNull(RemoteExecutionContext.jobId(), "Context must be cleared after failed job");
    }

    // ── LogRelayClient context-aware submit ───────────────────────────────────

    @Test
    void logRelayClient_contextAwareSubmit_usesCurrentJobId() throws Exception {
        // Start a minimal fabric to accept the POST
        BatonFabric fabric = new BatonFabric(0);
        List<LogRecord> received = new CopyOnWriteArrayList<>();
        fabric.setLogSink(received::add);

        LogRelayClient relay = new LogRelayClient(fabric.getOrchestratorUrl(), "test-node");
        relay.start();

        try {
            RemoteExecutionContext.set("test-node", "ctx-job-xyz");
            relay.submit("system-out", "-", "-", "a line written inside a job");
            relay.stop();  // final flush

            long deadline = System.currentTimeMillis() + 3_000;
            while (received.isEmpty() && System.currentTimeMillis() < deadline) Thread.sleep(20);

            assertFalse(received.isEmpty(), "Expected at least one record");
            assertEquals("ctx-job-xyz", received.get(0).jobId,
                    "submit() should use the current context jobId");
        } finally {
            RemoteExecutionContext.clear();
            fabric.close();
        }
    }

    @Test
    void logRelayClient_contextAwareSubmit_usesDashWhenNoContext() throws Exception {
        BatonFabric fabric = new BatonFabric(0);
        List<LogRecord> received = new CopyOnWriteArrayList<>();
        fabric.setLogSink(received::add);

        LogRelayClient relay = new LogRelayClient(fabric.getOrchestratorUrl(), "test-node");
        relay.start();

        try {
            // No context set — jobId should be "-"
            relay.submit("system-out", "-", "-", "line outside a job");
            relay.stop();

            long deadline = System.currentTimeMillis() + 3_000;
            while (received.isEmpty() && System.currentTimeMillis() < deadline) Thread.sleep(20);

            assertFalse(received.isEmpty());
            assertEquals("-", received.get(0).jobId);
        } finally {
            fabric.close();
        }
    }

    // ── ProcessLogRelay.attach() ──────────────────────────────────────────────

    @Test
    void processLogRelay_attach_usesCurrentContext() throws Exception {
        BatonFabric fabric = new BatonFabric(0);
        List<LogRecord> received = new CopyOnWriteArrayList<>();
        fabric.setLogSink(received::add);

        LogRelayClient relay = new LogRelayClient(fabric.getOrchestratorUrl(), "test-node");
        relay.start();

        try {
            RemoteExecutionContext.set("test-node", "attach-job-99");

            Process p = new ProcessBuilder(
                    ProcessHandle.current().info().command().orElse("java"),
                    "-cp", System.getProperty("java.class.path"),
                    LogRelayIT.PrintAndExit.class.getName())
                    .start();

            // attach() reads context — no explicit jobId needed
            ProcessLogRelay pRelay = ProcessLogRelay.attach(p, relay, "attached-proc");

            assertTrue(p.waitFor(10, TimeUnit.SECONDS));
            pRelay.awaitEof(5, TimeUnit.SECONDS);
            relay.stop();

            long deadline = System.currentTimeMillis() + 3_000;
            while (received.size() < 2 && System.currentTimeMillis() < deadline) Thread.sleep(20);

            assertTrue(received.size() >= 1);
            // All records should carry the context job ID
            for (LogRecord r : received) {
                assertEquals("attach-job-99", r.jobId,
                        "attach() should tag records with the current context jobId");
            }
        } finally {
            RemoteExecutionContext.clear();
            fabric.close();
        }
    }
}
