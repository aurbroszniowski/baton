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

        String hostname = InetAddress.getLocalHost().getHostName();
        int    pid      = (int) ProcessHandle.current().pid();

        NodeId selfId = new NodeId(name, hostname, port, pid);

        System.out.printf("[baton-agent][%s][-] Starting %s → orchestrator=%s%n", agentRunId, selfId, orchestratorUrl);

        JobRunner runner = new JobRunner(orchestratorUrl, agentRunId);

        // Shutdown hook shared by AgentServer (HTTP /shutdown) and JVM shutdown
        Object shutdownLock = new Object();
        Runnable shutdown = () -> {
            synchronized (shutdownLock) { shutdownLock.notifyAll(); }
        };

        AgentServer agentServer = new AgentServer(port, runner, shutdown);

        // Register with orchestrator
        register(orchestratorUrl, selfId);

        // Start heartbeat
        HeartbeatReporter heartbeat = new HeartbeatReporter(orchestratorUrl, selfId, agentRunId);
        heartbeat.start();

        System.out.printf("[baton-agent][%s][-] Ready on port %d%n", agentRunId, agentServer.getPort());

        // Block until shutdown
        synchronized (shutdownLock) {
            shutdownLock.wait();
        }

        System.out.printf("[baton-agent][%s][-] Shutting down%n", agentRunId);
        heartbeat.stop();
        agentServer.stop();
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
