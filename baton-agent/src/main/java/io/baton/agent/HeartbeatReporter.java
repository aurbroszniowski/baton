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
 * Sends a {@code POST /agent/heartbeat} to the orchestrator every 5 seconds so that
 * {@link io.baton.core.AgentRegistry} can detect dead agents.
 */
class HeartbeatReporter {

    private static final long INTERVAL_MS = 5_000;

    private final String    orchestratorUrl;
    private final NodeId    selfId;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "baton-heartbeat-sender");
        t.setDaemon(true);
        return t;
    });

    HeartbeatReporter(String orchestratorUrl, NodeId selfId) {
        this.orchestratorUrl = orchestratorUrl;
        this.selfId          = selfId;
    }

    void start() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    void stop() {
        scheduler.shutdownNow();
    }

    private void sendHeartbeat() {
        try {
            post(orchestratorUrl + "/agent/heartbeat",
                    nodeIdToString(selfId).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("[baton-agent] Heartbeat failed: " + e.getMessage());
        }
    }

    static String nodeIdToString(NodeId n) {
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
