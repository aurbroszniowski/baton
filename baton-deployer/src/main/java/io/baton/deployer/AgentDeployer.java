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
package io.baton.deployer;

import io.baton.NodeId;
import io.baton.SshConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * SSH-based lifecycle manager for remote baton agents.
 *
 * <p>Uses <a href="https://github.com/hierynomus/sshj">sshj</a> for SSH/SCP transport.
 *
 * <pre>
 * AgentDeployer deployer = new AgentDeployer(Path.of("baton-agent-fat.jar"));
 * NodeId node = deployer.deploy("server-a", SshConfig.of("user", "~/.ssh/id_rsa"),
 *                               "http://orchestrator:9400");
 * // ... run jobs ...
 * deployer.stop(node);
 * </pre>
 */
public class AgentDeployer {

    private static final String REMOTE_DIR = "~/baton";
    private static final String REMOTE_JAR = REMOTE_DIR + "/agent.jar";

    private final Path agentJar;

    public AgentDeployer(Path agentJar) {
        this.agentJar = agentJar;
    }

    /**
     * SCPs the agent JAR, starts the agent process, and waits up to 30 s for it to
     * register with the orchestrator.
     *
     * @return the {@link NodeId} the agent registered under
     */
    public NodeId deploy(String hostname, SshConfig ssh, String orchestratorUrl)
            throws IOException, InterruptedException, TimeoutException {
        int agentPort = 8700; // TODO: pick a free remote port

        upload(hostname, ssh, agentJar);
        start(hostname, ssh, orchestratorUrl, agentPort);
        return awaitRegistration(hostname, agentPort, 30_000);
    }

    /** Sends {@code POST /shutdown} to the agent. */
    public void stop(NodeId node) throws IOException {
        HttpURLConnection conn = (HttpURLConnection)
                new URL("http://" + node.getHostname() + ":" + node.getPort() + "/shutdown").openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().write(new byte[0]);
        conn.getResponseCode();
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private void upload(String hostname, SshConfig ssh, Path jar) throws IOException {
        try (SSHClient client = newSshClient(hostname, ssh)) {
            exec(client, "mkdir -p " + REMOTE_DIR);
            client.newSCPFileTransfer().upload(new FileSystemFile(jar.toFile()), REMOTE_JAR);
        }
    }

    private void start(String hostname, SshConfig ssh, String orchestratorUrl, int port) throws IOException {
        String cmd = "nohup java -jar " + REMOTE_JAR
                + " --orchestrator " + orchestratorUrl
                + " --port " + port
                + " > " + REMOTE_DIR + "/agent.log 2>&1 &";
        try (SSHClient client = newSshClient(hostname, ssh)) {
            exec(client, cmd);
        }
    }

    private void exec(SSHClient client, String command) throws IOException {
        try (Session session = client.startSession()) {
            Session.Command cmd = session.exec(command);
            cmd.join(30, TimeUnit.SECONDS);
        }
    }

    private NodeId awaitRegistration(String hostname, int port, long timeoutMs)
            throws InterruptedException, TimeoutException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String healthUrl = "http://" + hostname + ":" + port + "/health";
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(healthUrl).openConnection();
                conn.setConnectTimeout(1_000);
                conn.setReadTimeout(1_000);
                if (conn.getResponseCode() == 200) {
                    return new NodeId("agent-" + port, hostname, port, -1);
                }
            } catch (IOException ignored) {
                // Not up yet
            }
            Thread.sleep(500);
        }
        throw new TimeoutException(
                "Agent on " + hostname + ":" + port + " did not come up within " + timeoutMs + " ms");
    }

    private SSHClient newSshClient(String hostname, SshConfig ssh) throws IOException {
        SSHClient client = new SSHClient();
        client.addHostKeyVerifier(new PromiscuousVerifier()); // TODO: use known_hosts in production
        client.connect(hostname, ssh.getPort());
        client.authPublickey(ssh.getUsername(), ssh.getIdentityFile());
        return client;
    }
}
