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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatonAgentFabricRegistrationTest {

    @Test
    void registrationFailureStopsStartedAgentServer() throws Exception {
        ServerSocket serverSocket = new ServerSocket(0);
        int orchestratorPort = serverSocket.getLocalPort();
        int agentPort = freePort();
        CountDownLatch accepted = new CountDownLatch(1);

        Thread closingServer = new Thread(() -> {
            try (ServerSocket ignoredServer = serverSocket;
                 Socket ignored = ignoredServer.accept()) {
                accepted.countDown();
            } catch (IOException ignored) {
                accepted.countDown();
            }
        }, "closing-registration-server");
        closingServer.start();

        assertThrows(IOException.class, () ->
                new BatonAgentFabric("http://localhost:" + orchestratorPort, agentPort, "failing-agent"));
        assertTrue(accepted.await(5, TimeUnit.SECONDS), "registration attempt should reach the orchestrator");

        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && isAgentReachable(agentPort)) {
            Thread.sleep(100);
        }
        assertFalse(isAgentReachable(agentPort), "agent server must be stopped after registration failure");
    }

    @Test
    void invalidRegistrationBodyReturnsHttp400() throws Exception {
        AgentRegistry registry = new AgentRegistry();
        BatonServer server = new BatonServer(
                new PrimitivesStore(),
                new BarrierCoordinator(),
                new QueueStore(),
                registry,
                new JobDispatcher(),
                new LogCollector(record -> {}),
                0);
        try {
            HttpURLConnection conn = (HttpURLConnection)
                    new URL("http://localhost:" + server.getPort() + "/agent/register").openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.getOutputStream().write("not-enough-fields".getBytes());

            assertEquals(400, conn.getResponseCode());
        } finally {
            server.stop();
            registry.shutdown();
        }
    }

    private static boolean isAgentReachable(int port) {
        try {
            HttpURLConnection conn = (HttpURLConnection)
                    new URL("http://localhost:" + port + "/health").openConnection();
            conn.setConnectTimeout(500);
            conn.setReadTimeout(500);
            return conn.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
