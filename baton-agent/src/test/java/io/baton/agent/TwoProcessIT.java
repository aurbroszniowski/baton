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
package io.baton.agent;

import io.baton.DistributedCounter;
import io.baton.NodeId;
import io.baton.core.BatonFabric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2 + Phase 3 — two local processes communicating via HTTP.
 *
 * <p>The orchestrator runs in this JVM; the agent runs in a child process
 * started with {@link ProcessBuilder} using the same classpath.  All
 * communication is over the real HTTP server.
 *
 * <p>Phase 2 tests verify registration and heartbeat.
 * Phase 3 tests ship lambdas ({@link ClassBundle}) to the agent and assert
 * the result comes back over the result-callback flow.
 */
class TwoProcessIT {

    private BatonFabric fabric;
    private Process     agentProcess;

    @BeforeEach
    void setUp() throws Exception {
        fabric = new BatonFabric(0); // starts HTTP server on a random free port
    }

    @AfterEach
    void tearDown() {
        if (agentProcess != null) agentProcess.destroyForcibly();
        fabric.close();
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    @Test
    void agent_registersWithOrchestrator() throws Exception {
        int orchestratorPort = fabric.getOrchestratorPort();
        int agentPort        = freePort();

        agentProcess = startAgent(orchestratorPort, agentPort, "test-agent");

        NodeId registered = awaitRegistration("test-agent", 10_000);
        assertNotNull(registered, "Agent should register within 10 s");
        assertEquals("test-agent", registered.getName());
        assertEquals(agentPort,    registered.getPort());
    }

    @Test
    void agent_heartbeatKeepsItAlive() throws Exception {
        int orchestratorPort = fabric.getOrchestratorPort();
        int agentPort        = freePort();

        agentProcess = startAgent(orchestratorPort, agentPort, "hb-agent");

        NodeId registered = awaitRegistration("hb-agent", 10_000);
        assertNotNull(registered, "Agent should register within 10 s");

        // Wait long enough for at least one heartbeat to be sent (every 5 s)
        Thread.sleep(6_000);

        boolean stillConnected = fabric.getConnectedNodes().stream()
                .anyMatch(n -> "hb-agent".equals(n.getName()));
        assertTrue(stillConnected, "Agent should remain connected after heartbeat");
    }

    // ── Phase 3 — remote lambda execution ─────────────────────────────────────

    @Test
    void remoteCallable_returnsValue() throws Exception {
        NodeId agent = startAndAwaitAgent("rc-agent");

        Future<Integer> f = fabric.executeAsync(agent, () -> 42);

        assertEquals(42, f.get(10, TimeUnit.SECONDS));
    }

    @Test
    void remoteCallable_capturesString() throws Exception {
        NodeId agent = startAndAwaitAgent("cap-agent");

        String msg = "hello from agent";
        @SuppressWarnings("unchecked")
        Future<String> f = (Future<String>) (Future<?>) fabric.executeAsync(agent, () -> msg.toUpperCase());

        assertEquals("HELLO FROM AGENT", f.get(10, TimeUnit.SECONDS));
    }

    @Test
    void remoteRunnable_mutatesHttpCounter() throws Exception {
        // counter() in HTTP mode returns HttpCounterProxy — serializable, calls back to orchestrator
        DistributedCounter counter = fabric.counter("remote-c", 0L);
        NodeId agent = startAndAwaitAgent("mut-agent");

        Future<Void> f = fabric.executeAsync(agent, () -> { counter.incrementAndGet(); });
        f.get(10, TimeUnit.SECONDS);

        assertEquals(1L, counter.get());
    }

    @Test
    void remoteCallable_exceptionPropagatesAsExecutionException() throws Exception {
        NodeId agent = startAndAwaitAgent("ex-agent");

        Future<Void> f = fabric.executeAsync(agent, () -> {
            throw new IllegalStateException("boom from agent");
        });

        assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
    }

    // ── Phase 6 — agent cleanup on Fabric.close() ─────────────────────────────

    @Test
    void fabricClose_shutsDownConnectedAgent() throws Exception {
        NodeId agent = startAndAwaitAgent("close-agent");
        String healthUrl = "http://localhost:" + agent.getPort() + "/health";
        assertTrue(isAgentReachable(healthUrl), "Agent should be up before close");

        fabric.close(); // should send POST /shutdown to the agent

        // Wait up to 5 s for the agent to stop responding
        long deadline = System.currentTimeMillis() + 5_000;
        boolean stopped = false;
        while (System.currentTimeMillis() < deadline) {
            if (!isAgentReachable(healthUrl)) { stopped = true; break; }
            Thread.sleep(100);
        }
        assertTrue(stopped, "Agent should have shut down after Fabric.close()");
    }

    // ── Phase 5 — file upload / download ──────────────────────────────────────

    @Test
    void fileUpload_thenDownload_contentMatches(@TempDir Path tempDir) throws Exception {
        NodeId agent = startAndAwaitAgent("file-agent");

        // Create a local file to upload
        Path localFile = tempDir.resolve("upload.txt");
        byte[] content = "hello from phase 5".getBytes();
        Files.write(localFile, content);

        // Upload to a path under the agent's temp directory
        String remotePath = tempDir.toAbsolutePath() + "/remote-copy.txt";
        fabric.upload(agent, localFile, remotePath);

        // Download it back to a different local path
        Path downloaded = tempDir.resolve("downloaded.txt");
        fabric.download(agent, remotePath, downloaded);

        assertArrayEquals(content, Files.readAllBytes(downloaded));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Convenience: start an agent and block until it registers; fails the test on timeout. */
    private NodeId startAndAwaitAgent(String name) throws Exception {
        int orchestratorPort = fabric.getOrchestratorPort();
        int agentPort        = freePort();
        agentProcess = startAgent(orchestratorPort, agentPort, name);
        NodeId node = awaitRegistration(name, 10_000);
        assertNotNull(node, "Agent '" + name + "' did not register within 10 s");
        return node;
    }

    private Process startAgent(int orchestratorPort, int agentPort, String name) throws Exception {
        String javaCmd   = ProcessHandle.current().info().command()
                .orElseThrow(() -> new IllegalStateException("Cannot determine java command"));
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaCmd, "-cp", classpath,
                "io.baton.agent.AgentMain",
                "--orchestrator", "http://localhost:" + orchestratorPort,
                "--port",          String.valueOf(agentPort),
                "--name",          name
        );
        pb.redirectErrorStream(true); // merge stdout + stderr
        return pb.start();
    }

    /** Blocks until a node with {@code name} appears in connected nodes, or returns null on timeout. */
    private NodeId awaitRegistration(String name, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (NodeId node : fabric.getConnectedNodes()) {
                if (name.equals(node.getName())) return node;
            }
            Thread.sleep(100);
        }
        return null;
    }

    /** Returns true if the agent's /health endpoint responds with HTTP 200. */
    private static boolean isAgentReachable(String healthUrl) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(healthUrl).openConnection();
            conn.setConnectTimeout(500);
            conn.setReadTimeout(500);
            return conn.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    /** Allocates a free port by binding and immediately releasing a ServerSocket. */
    private static int freePort() throws Exception {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }
}
