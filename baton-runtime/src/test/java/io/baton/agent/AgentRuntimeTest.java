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

import com.sun.net.httpserver.HttpServer;
import io.baton.ClassBundle;
import io.baton.NodeId;
import io.baton.RemoteCallable;
import io.baton.transport.HttpTransport;
import io.baton.transport.Serialization;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AgentRuntimeTest {
    @Test
    void identityUsesBoundPortAndShutdownBeforeWaitingIsRemembered() throws Exception {
        HttpServer orchestrator = registrationServer(200);
        try (AgentRuntime runtime = new AgentRuntime(0, "worker", "localhost")) {
            NodeId node = runtime.getNodeId();
            assertTrue(node.getPort() > 0);
            String endpoint = "http://localhost:" + node.getPort();
            assertThrows(IOException.class, () -> HttpTransport.get(endpoint + "/health"));
            assertThrows(IOException.class, () -> HttpTransport.get(endpoint + "/identity"));
            assertThrows(IOException.class, () -> HttpTransport.post(endpoint + "/job"));
            assertThrows(IllegalStateException.class, () -> runtime.execute(() -> 42));
            runtime.start("http://localhost:" + orchestrator.getAddress().getPort());
            assertThrows(IllegalStateException.class, () -> runtime.start("http://localhost:1"));
            assertEquals(node, Serialization.read(HttpTransport.getBytes("http://localhost:" + node.getPort()
                    + "/identity", 1_000)));
            HttpTransport.post("http://localhost:" + node.getPort() + "/shutdown");
            assertTimeoutPreemptively(Duration.ofSeconds(2), runtime::awaitShutdown);
            assertThrows(IOException.class, () -> HttpTransport.get("http://localhost:" + node.getPort() + "/health"));
        } finally {
            orchestrator.stop(0);
        }
    }

    @Test
    void rejectedRegistrationReleasesTheAgentPort() throws Exception {
        HttpServer orchestrator = registrationServer(500);
        try (AgentRuntime runtime = new AgentRuntime(0, "rejected", "localhost")) {
            int port = runtime.getNodeId().getPort();
            assertThrows(IOException.class, () -> runtime.start("http://localhost:" + orchestrator.getAddress().getPort()));
            try (ServerSocket rebound = new ServerSocket(port)) { assertEquals(port, rebound.getLocalPort()); }
        } finally {
            orchestrator.stop(0);
        }
    }

    @Test
    void closeBeforeStartupReleasesWaitersAndPreventsRegistration() throws Exception {
        try (AgentRuntime runtime = new AgentRuntime(0, "closed", "localhost")) {
            runtime.close();
            assertTimeoutPreemptively(Duration.ofSeconds(2), runtime::awaitShutdown);
            assertThrows(IllegalStateException.class, () -> runtime.start("http://localhost:1"));
        }
    }

    @Test
    void firstJobCanArriveBeforeTheRegistrationResponse() throws Exception {
        AtomicReference<String> accepted = new AtomicReference<>();
        AtomicReference<Object> result = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(1);
        HttpServer orchestrator = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        RemoteCallable<Integer> job = () -> 42;
        try (ObjectOutputStream out = new ObjectOutputStream(payload)) {
            out.writeUTF("first-job");
            out.writeObject(new ClassBundle(Map.of(), Serialization.write(job)));
        }
        orchestrator.createContext("/agent/register", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int port = Integer.parseInt(body.split(" ")[2]);
            accepted.set(HttpTransport.post("http://localhost:" + port + "/job", payload.toByteArray()));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        orchestrator.createContext("/agent/result/first-job", exchange -> {
            try { result.set(Serialization.read(exchange.getRequestBody().readAllBytes())); }
            catch (ClassNotFoundException e) { throw new IOException(e); }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
            completed.countDown();
        });
        orchestrator.start();
        try (AgentRuntime runtime = new AgentRuntime(0, "worker", "localhost")) {
            runtime.start("http://localhost:" + orchestrator.getAddress().getPort());
            assertEquals("accepted", accepted.get());
            assertTrue(completed.await(2, TimeUnit.SECONDS));
            assertEquals(42, result.get());
        } finally {
            orchestrator.stop(0);
        }
    }

    private static HttpServer registrationServer(int status) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/agent/register", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
        return server;
    }
}
