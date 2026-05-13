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

import io.baton.core.BatonFabric;
import io.baton.core.LogRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link LogRelayClient} and {@link ProcessLogRelay}.
 *
 * <p>A real {@link BatonFabric} starts the orchestrator HTTP server in this JVM;
 * {@link LogRelayClient} connects to it and POSTs log records.
 */
class LogRelayIT {

    private BatonFabric             fabric;
    private String                  orchestratorUrl;
    private List<LogRecord>         received;
    private LogRelayClient          relay;

    @BeforeEach
    void setUp() {
        received = new CopyOnWriteArrayList<>();
        fabric   = new BatonFabric(0);
        fabric.setLogSink(received::add);
        orchestratorUrl = fabric.getOrchestratorUrl();

        relay = new LogRelayClient(orchestratorUrl, "test-agent@localhost:0#0");
        relay.start();
    }

    @AfterEach
    void tearDown() {
        relay.stop();
        fabric.close();
    }

    // LogRelayClient 

    @Test
    void relay_submitAndFlush_recordArrivesAtOrchestrator() throws Exception {
        relay.submit("job42", "process", "my-server", "stdout", "hello from remote");
        relay.stop(); // triggers final flush; already stopped, second call is no-op

        waitForRecords(1, 3_000);

        assertEquals(1, received.size());
        LogRecord r = received.get(0);
        assertEquals("test-agent@localhost:0#0", r.nodeId);
        assertEquals("job42",                   r.jobId);
        assertEquals("process",                 r.source);
        assertEquals("my-server",               r.label);
        assertEquals("stdout",                  r.stream);
        assertEquals("hello from remote",       r.line);
    }

    @Test
    void relay_multipleLines_allArriveInOrder() throws Exception {
        for (int i = 0; i < 10; i++) {
            relay.submit("-", "system-out", "-", "-", "line-" + i);
        }
        relay.stop();

        waitForRecords(10, 3_000);

        assertEquals(10, received.size());
        for (int i = 0; i < 10; i++) {
            assertEquals("line-" + i, received.get(i).line,
                    "Wrong line at index " + i);
        }
    }

    @Test
    void relay_failureDoesNotThrow() {
        // Stop the orchestrator but keep sending — must not throw
        fabric.close();

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 5; i++) {
                relay.submit("-", "process", "server", "stderr", "line " + i);
            }
            relay.stop();
        });
    }

    @Test
    void relay_dropCountIncrements_whenQueueFull() throws Exception {
        // Create a relay with a tiny queue via a subclass override is not possible,
        // so we flood the default 4000-entry queue with 5000 items before the flush
        // thread has a chance to drain it.  In practice the queue will be drained
        // fast, so this test just verifies that submit() never blocks.
        LogRelayClient tiny = new LogRelayClient(orchestratorUrl, "tiny-agent@localhost:0#0") {
            // No override needed — just verify the operation completes quickly
        };
        // Do NOT start() so the flush thread never runs; queue fills up.
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5_000; i++) {
            tiny.submit("-", "process", "s", "stdout", "line " + i);
        }
        long elapsed = System.currentTimeMillis() - start;
        // 5000 non-blocking offers should complete in well under 1 second
        assertTrue(elapsed < 1_000, "submit() appears to be blocking; elapsed=" + elapsed + " ms");
        // Drop count must be > 0 (queue capacity is 4000, we submitted 5000)
        assertTrue(tiny.getDropCount() > 0,
                "Expected drops, got " + tiny.getDropCount());
    }

    // ProcessLogRelay 

    @Test
    void processRelay_capturesStdoutAndStderr() throws Exception {
        // Start a child process that prints to both streams then exits
        ProcessBuilder pb = new ProcessBuilder(
                ProcessHandle.current().info().command().orElse("java"),
                "-cp", System.getProperty("java.class.path"),
                // Inline script: use a helper class that prints and exits
                PrintAndExit.class.getName());
        pb.redirectErrorStream(false); // keep streams separate

        // Actually we need a main class — use a different approach:
        // spawn a subprocess via ProcessBuilder that just echoes lines
        Process process = new ProcessBuilder(
                findJava(),
                "-cp", System.getProperty("java.class.path"),
                PrintAndExit.class.getName())
                .start();

        ProcessLogRelay processRelay = new ProcessLogRelay(process, relay, "test-job", "echo-proc");

        // Wait for process and streams
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Process did not exit");
        processRelay.awaitEof(5, TimeUnit.SECONDS);
        relay.stop();

        waitForRecords(2, 3_000);

        List<String> lines = new ArrayList<>();
        for (LogRecord r : received) lines.add(r.line);

        assertTrue(lines.contains("stdout-line"), "Expected 'stdout-line' in " + lines);
        assertTrue(lines.contains("stderr-line"), "Expected 'stderr-line' in " + lines);

        // Verify metadata
        for (LogRecord r : received) {
            assertEquals("process",   r.source);
            assertEquals("echo-proc", r.label);
            assertEquals("test-job",  r.jobId);
        }
    }

    @Test
    void processRelay_relayFailureDoesNotFailProcess() throws Exception {
        // Stop the orchestrator so relay POSTs fail
        fabric.close();

        Process process = new ProcessBuilder(findJava(),
                "-cp", System.getProperty("java.class.path"),
                PrintAndExit.class.getName())
                .start();

        ProcessLogRelay processRelay = new ProcessLogRelay(process, relay, "-", "echo-proc");

        // Process should complete normally even though relay is broken
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Process did not exit");
        assertDoesNotThrow(() -> processRelay.awaitEof(5, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue(), "Process exit code should be 0");
    }

    // Helpers 

    /** Waits up to {@code timeoutMs} for at least {@code count} records to arrive. */
    private void waitForRecords(int count, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (received.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    private static String findJava() {
        return ProcessHandle.current().info().command().orElse("java");
    }

    // Helper process main class 

    /** Prints one line to stdout and one to stderr, then exits. */
    public static class PrintAndExit {
        public static void main(String[] args) {
            System.out.println("stdout-line");
            System.err.println("stderr-line");
        }
    }
}
