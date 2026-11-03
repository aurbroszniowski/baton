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
import io.baton.AgentLogRelay;
import io.baton.BatonRuntime;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatonAgentFabricRegistrationTest {

    @Test
    void registrationFailureStopsStartedAgentServer() throws Exception {
        AtomicInteger agentPort = new AtomicInteger();
        HttpServer orchestrator = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        orchestrator.createContext("/agent/register", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            agentPort.set(Integer.parseInt(body.split(" ")[2]));
            exchange.close(); // Drop the registration response to simulate transport failure.
        });
        orchestrator.start();
        try {
            assertThrows(IOException.class, () -> new BatonAgentFabric("http://localhost:"
                    + orchestrator.getAddress().getPort(), 0, "failing-agent"));
            assertTrue(agentPort.get() > 0, "registration attempt should reach the orchestrator");
            assertFalse(isAgentReachable(agentPort.get()), "agent server must be stopped after registration failure");
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            PrintStream original = System.out;
            try (PrintStream captured = new PrintStream(output, true, StandardCharsets.UTF_8)) {
                System.setOut(captured);
                BatonRuntime.emit("framework", "startup", "-", "log after failed startup");
            } finally {
                System.setOut(original);
            }
            assertEquals("", output.toString(StandardCharsets.UTF_8), "Failed startup must reset the relay to NOOP");
        } finally {
            BatonRuntime.install(AgentLogRelay.NOOP);
            orchestrator.stop(0);
        }
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

}
