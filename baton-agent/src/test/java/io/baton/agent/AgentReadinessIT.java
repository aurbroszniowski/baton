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

import io.baton.AgentEndpoint;
import io.baton.DeploymentOptions;
import io.baton.NodeId;
import io.baton.SshConfig;
import io.baton.agent.fixtures.SlowInitializer;
import io.baton.core.BatonFabric;
import io.baton.deployer.AgentDeployer;
import io.baton.transport.HttpTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class AgentReadinessIT {
    @Test
    void registrationAndCompatibilityIdentityWaitForRemoteInitializers(@TempDir Path directory) throws Exception {
        Path fixture = initializerJar(directory);
        Path agentJar = Path.of(System.getProperty("baton.test.agentJar"));
        ExecutorService observers = Executors.newFixedThreadPool(2);
        try (BatonFabric fabric = new BatonFabric(0)) {
            Future<Boolean> firstJob = observers.submit(() -> {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (System.nanoTime() < deadline) {
                    for (NodeId node : fabric.getConnectedNodes()) {
                        return fabric.executeAsync(node, SlowInitializer.probe()).get(5, TimeUnit.SECONDS);
                    }
                    Thread.sleep(1);
                }
                throw new IllegalStateException("Agent did not register");
            });
            Process process = new ProcessBuilder(
                    ProcessHandle.current().info().command().orElseThrow(),
                    "-Dbaton.agent.workDir=" + directory,
                    "-cp", agentJar + java.io.File.pathSeparator + fixture,
                    "io.baton.agent.AgentMain", "--orchestrator", fabric.getOrchestratorUrl(),
                    "--port", "0", "--hostname", "localhost", "--name", "slow-initializer")
                    .redirectErrorStream(true).redirectOutput(directory.resolve("agent.log").toFile()).start();
            try {
                Path entered = directory.resolve("initializer.entered");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!Files.exists(entered) && process.isAlive() && System.nanoTime() < deadline) Thread.sleep(10);
                assertTrue(Files.exists(entered), () -> agentLog(directory));
                int port = Integer.parseInt(Files.readString(entered));
                String endpoint = "http://localhost:" + port;
                assertStatus(503, endpoint + "/health", "GET");
                assertStatus(503, endpoint + "/identity", "GET");
                assertStatus(503, endpoint + "/job", "POST");
                assertTrue(fabric.getConnectedNodes().isEmpty(), "Initializing agents must not be published");
                assertFalse(firstJob.isDone(), "No job can run until initializers complete");

                // Exercise the old deploy() polling path without requiring an SSH server.
                AgentDeployer compatibility = new AgentDeployer(agentJar) {
                    @Override
                    public AgentEndpoint start(String host, SshConfig ssh, String url, DeploymentOptions options) {
                        return new AgentEndpoint("localhost", port);
                    }
                };
                Future<NodeId> identity = observers.submit(() -> compatibility.deploy("localhost", null,
                        fabric.getOrchestratorUrl(), new DeploymentOptions(List.of(), Map.of(), "unused")));
                assertFalse(identity.isDone());
                Files.writeString(directory.resolve("initializer.release"), "release");
                assertTrue(firstJob.get(10, TimeUnit.SECONDS), "The first registered job must see initialized framework state");
                NodeId node = identity.get(10, TimeUnit.SECONDS);
                assertTrue(fabric.getConnectedNodes().contains(node));
                assertTrue(node.getPid() > 0);
                assertStatus(200, endpoint + "/health", "GET");
            } finally {
                Files.writeString(directory.resolve("initializer.release"), "release");
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        } finally {
            observers.shutdownNow();
        }
    }

    private static void assertStatus(int status, String url, String method) throws IOException {
        HttpURLConnection connection = HttpTransport.open(url, method, 1_000);
        try { assertEquals(status, connection.getResponseCode()); }
        finally { connection.disconnect(); }
    }

    private static String agentLog(Path directory) {
        try { return Files.readString(directory.resolve("agent.log")); }
        catch (IOException e) { return e.toString(); }
    }

    private static Path initializerJar(Path directory) throws IOException {
        Path jar = directory.resolve("initializer.jar");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            String resource = SlowInitializer.class.getName().replace('.', '/') + ".class";
            out.putNextEntry(new JarEntry(resource));
            try (InputStream in = SlowInitializer.class.getClassLoader().getResourceAsStream(resource)) {
                assertNotNull(in);
                in.transferTo(out);
            }
            out.closeEntry();
            out.putNextEntry(new JarEntry("META-INF/services/io.baton.RemoteInitializer"));
            out.write((SlowInitializer.class.getName() + "\n").getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return jar;
    }
}
