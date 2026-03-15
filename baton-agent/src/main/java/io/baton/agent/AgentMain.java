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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
 * The agent:
 * <ol>
 *   <li>Binds {@link AgentServer} on {@code --port}.
 *   <li>POSTs {@code /agent/register} to the orchestrator.
 *   <li>Starts {@link HeartbeatReporter} (every 5 s).
 *   <li>Blocks until {@code POST /shutdown} is received.
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

        // Kill all child processes (TC server, config-tool, client JVMs, …) when this
        // agent JVM exits — whether via graceful /shutdown, missed heartbeat, or crash.
        // Without this, failed test runs leave orphaned server processes on the remote
        // machine that interfere with subsequent runs.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ProcessHandle.current().descendants().forEach(child -> {
                System.out.printf("[baton-agent][%s][-] Shutdown hook: killing child process %d%n",
                        agentRunId, child.pid());
                child.destroyForcibly();
            });
        }, "baton-agent-child-killer"));

        JobRunner runner = new JobRunner(orchestratorUrl, agentRunId);

        // Shutdown hook shared by AgentServer (HTTP /shutdown) and JVM shutdown
        Object shutdownLock = new Object();
        Runnable shutdown = () -> {
            synchronized (shutdownLock) { shutdownLock.notifyAll(); }
        };

        AgentServer agentServer = new AgentServer(port, runner, shutdown);

        // Register with orchestrator
        register(orchestratorUrl, selfId);

        // Start heartbeat — self-shutdown if orchestrator goes silent
        HeartbeatReporter heartbeat = new HeartbeatReporter(orchestratorUrl, selfId, agentRunId, shutdown);
        heartbeat.start();

        System.out.printf("[baton-agent][%s][-] Ready on port %d%n", agentRunId, agentServer.getPort());

        // Optional Angela integration: initialize AgentController if Angela is on the classpath.
        // Done via reflection so baton-agent has no compile-time dependency on Angela.
        initAngela(agentRunId, name, hostname, agentServer.getPort(), pid);

        // Block until shutdown
        synchronized (shutdownLock) {
            shutdownLock.wait();
        }

        System.out.printf("[baton-agent][%s][-] Shutting down%n", agentRunId);
        heartbeat.stop();
        agentServer.stop();
    }

    /**
     * If Angela's agent-lib is on the classpath (it gets uploaded by AgentDeployer alongside
     * the baton fat-jar), initialize Angela's AgentController so that Angela jobs can call
     * AgentController.getInstance() on this remote node. Uses reflection to avoid a
     * compile-time dependency on Angela in baton-agent.
     */
    private static void initAngela(String agentRunId, String name, String hostname, int port, int pid) {
        try {
            Class<?> agentIdClass      = Class.forName("org.terracotta.angela.agent.com.AgentID");
            Class<?> portAllocIntf     = Class.forName("org.terracotta.angela.common.net.PortAllocator");
            Class<?> portAllocClass    = Class.forName("org.terracotta.angela.common.net.DefaultPortAllocator");
            Class<?> controllerClass   = Class.forName("org.terracotta.angela.agent.AgentController");

            Object agentId     = agentIdClass.getConstructor(String.class, String.class, int.class, int.class)
                                             .newInstance(name, hostname, port, pid);
            Object portAlloc   = portAllocClass.getConstructor().newInstance();
            Object controller  = controllerClass.getConstructor(agentIdClass, portAllocIntf)
                                                .newInstance(agentId, portAlloc);
            controllerClass.getMethod("setUniqueInstance", controllerClass).invoke(null, controller);

            System.out.printf("[baton-agent][%s][-] Angela AgentController initialized%n", agentRunId);
        } catch (ClassNotFoundException ignored) {
            // Angela not on classpath — pure baton mode, nothing to do
        } catch (Exception e) {
            System.err.printf("[baton-agent][%s][-] WARNING: Angela AgentController init failed: %s%n",
                              agentRunId, e);
        }
    }

    private static void register(String orchestratorUrl, NodeId selfId) throws IOException {
        String body = HeartbeatReporter.nodeIdToString(selfId);
        HttpURLConnection conn = (HttpURLConnection) new URL(orchestratorUrl + "/agent/register").openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        try (OutputStream out = conn.getOutputStream()) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
        int code = conn.getResponseCode();
        if (code != 200) throw new IOException("Orchestrator rejected registration: HTTP " + code);
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
