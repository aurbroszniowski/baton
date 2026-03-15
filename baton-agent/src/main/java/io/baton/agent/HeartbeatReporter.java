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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends a {@code POST /agent/heartbeat} to the orchestrator every 5 seconds.
 * If {@link #MAX_MISSED} consecutive heartbeats fail the {@code onOrchestatorGone}
 * callback is invoked, triggering an agent self-shutdown.
 */
public class HeartbeatReporter {

    private static final long INTERVAL_MS  = 5_000;
    private static final int  MAX_MISSED   = 3; // 15 s of silence → self-shutdown

    private final String    orchestratorUrl;
    private final NodeId    selfId;
    private final String    agentRunId;
    private final Runnable  onOrchestratorGone;
    private int             missedBeats = 0;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "baton-heartbeat-sender");
        t.setDaemon(true);
        return t;
    });

    public HeartbeatReporter(String orchestratorUrl, NodeId selfId, String agentRunId,
                             Runnable onOrchestratorGone) {
        this.orchestratorUrl    = orchestratorUrl;
        this.selfId             = selfId;
        this.agentRunId         = agentRunId;
        this.onOrchestratorGone = onOrchestratorGone;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    private void sendHeartbeat() {
        try {
            post(orchestratorUrl + "/agent/heartbeat",
                    nodeIdToString(selfId).getBytes(StandardCharsets.UTF_8));
            missedBeats = 0; // reset on success
        } catch (IOException e) {
            missedBeats++;
            System.err.printf("[baton-agent][%s][-] Heartbeat failed (%d/%d): %s%n",
                    agentRunId, missedBeats, MAX_MISSED, e.getMessage());
            if (missedBeats >= MAX_MISSED) {
                System.err.printf("[baton-agent][%s][-] Orchestrator unreachable — self-shutting down%n", agentRunId);
                scheduler.shutdownNow();
                onOrchestratorGone.run();
            }
        }
    }

    public static String nodeIdToString(NodeId n) {
        return n.getName() + " " + n.getHostname() + " " + n.getPort() + " " + n.getPid();
    }

    private void post(String url, byte[] body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        try (OutputStream out = conn.getOutputStream()) {
            out.write(body);
        }
        conn.getResponseCode();
    }
}
