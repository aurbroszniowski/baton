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

import io.baton.NodeId;
import io.baton.RemoteInitializer;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.UUID;

/**
 * Entry point for the baton agent fat-JAR.
 *
 * <pre>
 * java -jar baton-agent-fat.jar \
 *      --orchestrator http://192.168.1.10:9400 \
 *      --port 8700 \
 *      --name worker-0
 * </pre>
 *
 * Startup sequence:
 * <ol>
 *   <li>Start {@link LogRelayClient} and tee {@code System.out/err} via
 *       {@link SystemStreamCapture} so all output is forwarded from the start.</li>
 *   <li>Bind {@link AgentServer} on {@code --port} and establish the agent identity.</li>
 *   <li>Run all {@link RemoteInitializer} implementations found by
 *       {@link ServiceLoader} so frameworks can install logging appenders and
 *       initialize static state (Angela's {@code AgentController}, etc.).</li>
 *   <li>Call {@link AgentRuntime#start}, registering with the orchestrator, starting
 *       heartbeats, and announcing readiness.</li>
 *   <li>Block until {@code POST /shutdown} or orchestrator heartbeat timeout.</li>
 * </ol>
 */
public class AgentMain {

    public static void main(String[] args) throws Exception {
        String orchestratorUrl = requireArg(args, "--orchestrator");
        int    port            = Integer.parseInt(requireArg(args, "--port"));
        String name            = argOrDefault(args, "--name", "agent-" + port);
        String agentRunId      = UUID.randomUUID().toString().replace("-", "").substring(0, 8);

        String hostnameArg = argOrDefault(args, "--hostname", null);
        String hostname = (hostnameArg != null) ? hostnameArg : InetAddress.getLocalHost().getHostName();
        int    pid      = (int) ProcessHandle.current().pid();

        NodeId selfId = new NodeId(name, hostname, port, pid);

        System.out.printf("[baton-agent][%s][-] Starting %s -> orchestrator=%s%n", agentRunId, selfId, orchestratorUrl);

        // Start log relay and tee System.out/err before anything else so all
        // subsequent output — including startup messages — is forwarded.
        LogRelayClient logRelay = new LogRelayClient(orchestratorUrl, selfId.toLogTag());
        logRelay.start();
        BatonAgentRuntime.install(logRelay);
        SystemStreamCapture.install(logRelay);

        // Kill all child processes when this agent JVM exits — whether via
        // graceful /shutdown, missed heartbeat, or crash — to avoid orphaned
        // server processes interfering with subsequent test runs.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ProcessHandle.current().descendants().forEach(child -> {
                System.out.printf("[baton-agent][%s][-] Shutdown hook: killing child process %d%n",
                        agentRunId, child.pid());
                child.destroyForcibly();
            });
        }, "baton-agent-child-killer"));

        try (AgentRuntime runtime = new AgentRuntime(port, name, hostname)) {

            // Discover and run all RemoteInitializer implementations.  This is
            // the generic replacement for the former Angela-specific initAngela()
            // block.  Frameworks ship their own RemoteInitializer alongside the
            // baton agent on the remote classpath; baton never hard-codes them.
            Path workDir = resolveWorkDir(name);
            runInitializers(runtime.getNodeId(), workDir, agentRunId, ServiceLoader.load(RemoteInitializer.class));

            runtime.start(orchestratorUrl);
            System.out.printf("[baton-agent][%s][-] Ready on port %d%n", agentRunId, runtime.getNodeId().getPort());

            // Block until shutdown
            runtime.awaitShutdown();

            System.out.printf("[baton-agent][%s][-] Shutting down%n", agentRunId);
        } finally {
            logRelay.stop();
        }
    }

    // Package-private helpers (visible to tests) 

    /**
     * Runs all {@link RemoteInitializer} implementations in {@code initializers}.
     * Exceptions from individual initializers are caught and logged; they never
     * abort startup.  In production, {@code initializers} is
     * {@code ServiceLoader.load(RemoteInitializer.class)}.
     */
    static void runInitializers(NodeId agentId, Path workDir, String agentRunId,
                                Iterable<RemoteInitializer> initializers) {
        for (RemoteInitializer init : initializers) {
            try {
                System.out.printf("[baton-agent][%s][-] Running RemoteInitializer: %s%n",
                        agentRunId, init.getClass().getName());
                init.initialize(agentId, workDir);
            } catch (Exception e) {
                System.err.printf("[baton-agent][%s][-] RemoteInitializer %s failed: %s%n",
                        agentRunId, init.getClass().getName(), e);
            }
        }
    }

    // Private helpers 

    private static Path resolveWorkDir(String agentName) {
        Path dir = Path.of(System.getProperty("baton.agent.workDir",
                Path.of(System.getProperty("user.home"), "baton", agentName).toString()));
        try {
            Files.createDirectories(dir);
            for (String key : System.getProperties().stringPropertyNames()) {
                if (key.startsWith("baton.path.")) Files.createDirectories(Path.of(System.getProperty(key)));
            }
        } catch (IOException ignored) {}
        return dir;
    }

    private static String requireArg(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (flag.equals(args[i])) return args[i + 1];
        }
        throw new IllegalArgumentException("Missing required flag: " + flag);
    }

    private static String argOrDefault(String[] args, String flag, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (flag.equals(args[i])) return args[i + 1];
        }
        return def;
    }
}
