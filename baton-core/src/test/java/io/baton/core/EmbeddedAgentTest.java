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
import io.baton.NodeId;
import io.baton.RemoteExecutionContext;
import io.baton.transport.HttpTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class EmbeddedAgentTest {
    @Test
    void relayIsInstalledBeforeRegistrationIsPublished() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream original = System.out;
        HttpServer orchestrator = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        orchestrator.createContext("/agent/register", exchange -> {
            exchange.getRequestBody().readAllBytes();
            // Probe the JVM-wide hook while runtime.start() is still waiting for registration.
            BatonRuntime.emit("framework", "startup", "-", "log during registration");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        orchestrator.start();
        BatonRuntime.install(AgentLogRelay.NOOP);
        try (PrintStream captured = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(captured);
            try (BatonAgentFabric agent = new BatonAgentFabric("http://localhost:"
                    + orchestrator.getAddress().getPort(), 0, "early-log")) {
                assertTrue(output.toString(StandardCharsets.UTF_8).contains("log during registration"));
            }
        } finally {
            System.setOut(original);
            BatonRuntime.install(AgentLogRelay.NOOP);
            orchestrator.stop(0);
        }
    }

    @Test
    void embeddedAgentSharesExecutionFileTransferAndShutdownWithStandaloneRuntime(@TempDir Path directory) throws Exception {
        try (BatonFabric fabric = new BatonFabric(0);
             BatonAgentFabric agent = new BatonAgentFabric(fabric.getOrchestratorUrl(), 0, "embedded")) {
            NodeId node = agent.getLocalNodeId();
            assertTrue(fabric.getConnectedNodes().contains(node));
            assertEquals(node.toString(), fabric.executeAsync(node, RemoteExecutionContext::nodeId).get(5, TimeUnit.SECONDS));
            assertEquals(node.toString(), agent.executeAsync(node, RemoteExecutionContext::nodeId).get(5, TimeUnit.SECONDS));
            assertThrows(IllegalArgumentException.class,
                    () -> agent.executeAsync(fabric.getLocalNodeId(), () -> 42));

            assertEquals(7, agent.counter("embedded counter", 7).get());
            assertEquals("initial", agent.reference("embedded ref", "initial").get());
            Path source = directory.resolve("source.sh");
            Files.writeString(source, "#!/bin/sh\necho hello\n");
            String destination = directory.resolve("file + % & name.sh").toString();
            fabric.upload(node, source, destination);
            Path downloaded = directory.resolve("downloaded.sh");
            fabric.download(node, destination, downloaded);
            assertEquals(Files.readString(source), Files.readString(downloaded));

            fabric.disconnect(node);
            assertFalse(fabric.getConnectedNodes().contains(node));
            assertThrows(java.io.IOException.class,
                    () -> HttpTransport.get("http://" + node.getHostname() + ":" + node.getPort() + "/health"));
        }
    }
}
