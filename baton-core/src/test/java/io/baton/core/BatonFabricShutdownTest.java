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

import com.sun.net.httpserver.HttpServer;
import io.baton.NodeId;
import io.baton.agent.HeartbeatReporter;
import io.baton.transport.HttpTransport;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BatonFabricShutdownTest {
    @Test
    void severalUnresponsiveAgentsShareAShortShutdownBudget() throws Exception {
        List<HttpServer> servers = new ArrayList<>();
        List<ExecutorService> workers = new ArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try (BatonFabric fabric = new BatonFabric(0)) {
            try {
                for (int i = 0; i < 6; i++) {
                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
                    ExecutorService worker = Executors.newSingleThreadExecutor();
                    servers.add(server);
                    workers.add(worker);
                    server.setExecutor(worker);
                    server.createContext("/shutdown", exchange -> {
                        try { release.await(10, TimeUnit.SECONDS); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                        finally { exchange.close(); }
                    });
                    server.start();
                    NodeId node = new NodeId("stalled-" + i, "localhost", server.getAddress().getPort(), i + 1);
                    HttpTransport.post(fabric.getOrchestratorUrl() + "/agent/register",
                            HeartbeatReporter.nodeIdToString(node).getBytes(StandardCharsets.UTF_8));
                }
                assertEquals(6, fabric.getConnectedNodes().size());
                assertTimeoutPreemptively(Duration.ofSeconds(4), fabric::close);
                assertTrue(fabric.getConnectedNodes().isEmpty());
            } finally {
                release.countDown();
                servers.forEach(server -> server.stop(0));
                workers.forEach(ExecutorService::shutdownNow);
            }
        }
    }
}
