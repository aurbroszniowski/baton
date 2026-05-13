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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentMainRegistrationFailureTest {

    @Test
    void registrationFailureStopsAgentServerAndProcessExits(@TempDir Path tempDir) throws Exception {
        ServerSocket serverSocket = new ServerSocket(0);
        int orchestratorPort = serverSocket.getLocalPort();
        int agentPort = freePort();

        Thread closingServer = new Thread(() -> {
            try (ServerSocket ignoredServer = serverSocket;
                 Socket ignored = ignoredServer.accept()) {
                // Accept one connection, then close it without an HTTP response.
            } catch (IOException ignored) {
                // Test assertion is on the agent process cleanup.
            }
        }, "closing-registration-server");
        closingServer.start();

        Process process = new ProcessBuilder(
                javaCommand(),
                "-cp", System.getProperty("java.class.path"),
                "io.baton.agent.AgentMain",
                "--orchestrator", "http://localhost:" + orchestratorPort,
                "--port", String.valueOf(agentPort),
                "--name", "failing-agent",
                "--hostname", "localhost")
                .redirectErrorStream(true)
                .redirectOutput(tempDir.resolve("agent.log").toFile())
                .start();

        assertTrue(process.waitFor(15, TimeUnit.SECONDS), "agent process should exit after registration failure");
        assertNotEquals(0, process.exitValue(), "registration failure should keep a non-zero process exit");
        assertFalse(isAgentReachable(agentPort), "agent server must not be left running after registration failure");
    }

    private static String javaCommand() {
        return ProcessHandle.current().info().command()
                .orElseThrow(() -> new IllegalStateException("Cannot determine java command"));
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
