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
import io.baton.DistributedQueue;
import io.baton.DistributedReference;
import io.baton.NodeId;
import io.baton.core.BatonFabric;
import io.baton.RemoteExecutionContext;
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
 * started with {@link ProcessBuilder} from the standalone fat JAR, without the test or orchestrator classpath.  All
 * communication is over the real HTTP server.
 *
 * <p>Phase 2 tests verify registration and heartbeat.
 * Phase 3 tests ship lambdas ({@link ClassBundle}) to the agent and assert
 * the result comes back over the result-callback flow.
 */
class TwoProcessIT {

    @TempDir
    Path agentDirectory;

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

    // Tests 

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

    // Phase 3 — remote lambda execution 

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

        ExecutionException failure = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertEquals("boom from agent", failure.getCause().getMessage());
    }

    // Phase 4 — RemoteExecutionContext 

    @Test
    void remoteCallable_contextIsSetDuringExecution() throws Exception {
        NodeId agent = startAndAwaitAgent("ctx-set-agent");

        @SuppressWarnings("unchecked")
        Future<String> f = (Future<String>) (Future<?>) fabric.executeAsync(agent, () ->
                RemoteExecutionContext.jobId());

        String jobId = f.get(10, TimeUnit.SECONDS);
        assertNotNull(jobId, "RemoteExecutionContext.jobId() must not be null during execution");
        assertFalse(jobId.isEmpty(), "jobId must not be empty during execution");
    }

    @Test
    void remoteCallable_contextIsClearedBetweenJobs() throws Exception {
        NodeId agent = startAndAwaitAgent("ctx-clear-agent");

        // Two sequential jobs should each see their own distinct jobId
        @SuppressWarnings("unchecked")
        Future<String> f1 = (Future<String>) (Future<?>) fabric.executeAsync(agent, () ->
                RemoteExecutionContext.jobId());
        String jobId1 = f1.get(10, TimeUnit.SECONDS);

        @SuppressWarnings("unchecked")
        Future<String> f2 = (Future<String>) (Future<?>) fabric.executeAsync(agent, () ->
                RemoteExecutionContext.jobId());
        String jobId2 = f2.get(10, TimeUnit.SECONDS);

        assertNotNull(jobId1);
        assertNotNull(jobId2);
        assertNotEquals(jobId1, jobId2,
                "Each job must see its own unique jobId — not a stale value from a previous job");
    }

    @Test
    void remoteCallable_nodeIdMatchesAgentIdentity() throws Exception {
        NodeId agent = startAndAwaitAgent("nodeid-agent");

        @SuppressWarnings("unchecked")
        Future<String> f = (Future<String>) (Future<?>) fabric.executeAsync(agent, () ->
                RemoteExecutionContext.nodeId());

        String nodeId = f.get(10, TimeUnit.SECONDS);
        assertNotNull(nodeId, "nodeId must not be null during execution");
        // The nodeId should contain the agent's name
        assertTrue(nodeId.contains("nodeid-agent"),
                "nodeId '" + nodeId + "' should contain the agent name 'nodeid-agent'");
    }

    // Phase 6 — agent cleanup on Fabric.close() 

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
            if (agentProcess != null && !agentProcess.isAlive()) {
                fail("Agent exited before registration: " + new String(agentProcess.getInputStream().readAllBytes(),
                        java.nio.charset.StandardCharsets.UTF_8));
            }
            if (!isAgentReachable(healthUrl)) { stopped = true; break; }
            Thread.sleep(100);
        }
        assertTrue(stopped, "Agent should have shut down after Fabric.close()");
    }

    // Phase 5 — file upload / download 

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

    // Helpers 

    @Test
    void packagedAgentExecutesShippedClassesAndCapturedReferenceAndQueue() throws Exception {
        NodeId node = startAndAwaitAgent("shipped-agent");
        DistributedReference<String> reference = fabric.reference("reference / + % &", "initial");
        DistributedQueue<String> queue = fabric.queue("queue / + % &");
        ShippedValue captured = new ShippedValue("shipped");
        Future<String> result = fabric.executeAsync(node, () -> {
            // This class is absent from the agent JAR and must arrive in the bundle.
            if (ClassLoader.getSystemClassLoader().getResource("io/baton/agent/TwoProcessIT.class") != null) {
                throw new IllegalStateException("Agent inherited the test classpath");
            }
            reference.set(captured.value.toUpperCase());
            queue.put(reference.get());
            return queue.poll(2, TimeUnit.SECONDS);
        });
        assertEquals("SHIPPED", result.get(10, TimeUnit.SECONDS));
        assertEquals("SHIPPED", reference.get());
    }

    @Test
    void packagedAgentStreamsFilesAndPreservesPermissions(@TempDir Path directory) throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.getFileStore(directory).supportsFileAttributeView("posix"));
        NodeId node = startAndAwaitAgent("permissions-agent");
        Path source = directory.resolve("source.sh");
        Files.writeString(source, "#!/bin/sh\necho hello\n");
        java.util.Set<java.nio.file.attribute.PosixFilePermission> permissions =
                java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-x---");
        Files.setPosixFilePermissions(source, permissions);
        String remote = "data://remote + % & script.sh";
        fabric.upload(node, source, remote);
        assertEquals(permissions, Files.getPosixFilePermissions(agentDirectory.resolve("remote + % & script.sh")));
        Path downloaded = directory.resolve("downloaded.sh");
        fabric.download(node, remote, downloaded);
        assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(downloaded));
    }

    private static final class ShippedValue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final String value;
        ShippedValue(String value) { this.value = value; }
    }

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

        ProcessBuilder pb = new ProcessBuilder(
                javaCmd, "-Dbaton.agent.workDir=" + agentDirectory,
                "-Dbaton.path.data=" + agentDirectory,
                "-jar", System.getProperty("baton.test.agentJar"),
                "--orchestrator", "http://localhost:" + orchestratorPort,
                "--port",          String.valueOf(agentPort),
                "--name",          name,
                "--hostname",      "localhost"
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
