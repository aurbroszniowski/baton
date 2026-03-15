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

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

    private static final String REMOTE_SUBDIR = "baton";

    private final Path agentJar;

    public AgentDeployer(Path agentJar) {
        this.agentJar = agentJar;
    }

    /**
     * SCPs the agent JAR (plus any Angela jars found on the local classpath), starts the
     * agent process, and waits up to 30 s for it to register with the orchestrator.
     *
     * @return the {@link NodeId} the agent registered under
     */
    public NodeId deploy(String hostname, SshConfig ssh, String orchestratorUrl)
            throws IOException, InterruptedException, TimeoutException {
        int agentPort = 8700; // TODO: pick a free remote port

        // Shut down any stale agent from a previous (failed) run before deploying a fresh one.
        // Without this, awaitRegistration() returns the old agent's NodeId, which then misses
        // heartbeats on the new orchestrator and causes AgentDeadException mid-test.
        stopIfRunning(hostname, agentPort);

        String remoteDir;
        String localIp;
        try (SSHClient client = newSshClient(hostname, ssh)) {
            // The SSH socket's local address is the IP that the remote host can reach us at —
            // use it to override the orchestrator URL so the agent callback lands correctly.
            localIp = client.getLocalAddress().getHostAddress();
            String homeDir = execOutput(client, "echo $HOME").trim();
            remoteDir = homeDir + "/" + REMOTE_SUBDIR;
        }
        // Replace whatever host was in the URL with the SSH-proven local IP
        String effectiveOrchestratorUrl = orchestratorUrl.replaceFirst("http://[^:/]+", "http://" + localIp);
        System.out.println("[baton-deployer] SSH local address: " + localIp + " -> orchestrator URL: " + effectiveOrchestratorUrl);

        List<Path> extraJars = collectExtraJars();
        System.out.println("[baton-deployer] Uploading " + extraJars.size() + " extra jar(s) for remote classpath");

        upload(hostname, ssh, agentJar, remoteDir, extraJars);
        start(hostname, ssh, effectiveOrchestratorUrl, agentPort, remoteDir, extraJars);
        return awaitRegistration(hostname, agentPort, 30_000);
    }

    /**
     * Sends {@code POST /shutdown} to any agent already listening on {@code port} on the given
     * host, then waits up to 5 s for the port to be released.  Silently ignores connection
     * failures (meaning no agent is running there).
     */
    private void stopIfRunning(String hostname, int port) throws InterruptedException {
        String shutdownUrl = "http://" + hostname + ":" + port + "/shutdown";
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(shutdownUrl).openConnection();
            conn.setConnectTimeout(2_000);
            conn.setReadTimeout(2_000);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.getOutputStream().write(new byte[0]);
            int code = conn.getResponseCode();
            System.out.println("[baton-deployer] Sent shutdown to stale agent on " + hostname + ":" + port + " (HTTP " + code + ")");
            // Give the process a moment to release the port before we try to bind it again
            Thread.sleep(2_000);
        } catch (IOException ignored) {
            // No agent running on that port — nothing to do
        }
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

    /**
     * Returns all jars on the JVM classpath. The remote agent needs the same
     * dependencies as the local orchestrator process (Angela, slf4j, etc.) — the
     * only things already present on the remote are the JDK and the baton agent
     * fat jar itself.
     */
    private List<Path> collectExtraJars() {
        List<Path> result = new ArrayList<>();
        String cp = System.getProperty("java.class.path", "");
        for (String entry : cp.split(File.pathSeparator)) {
            Path p = Path.of(entry);
            if (Files.isRegularFile(p) && entry.endsWith(".jar")) {
                result.add(p);
            }
        }
        return result;
    }

    private void upload(String hostname, SshConfig ssh, Path jar, String remoteDir, List<Path> extraJars) throws IOException {
        try (SSHClient client = newSshClient(hostname, ssh)) {
            exec(client, "mkdir -p " + remoteDir + "/lib " + remoteDir + "/angela-root");
            client.newSCPFileTransfer().upload(new FileSystemFile(jar.toFile()), remoteDir + "/agent.jar");
            for (Path extra : extraJars) {
                client.newSCPFileTransfer().upload(
                        new FileSystemFile(extra.toFile()),
                        remoteDir + "/lib/" + extra.getFileName());
            }
        }
    }

    private String execOutput(SSHClient client, String command) throws IOException {
        try (Session session = client.startSession()) {
            Session.Command cmd = session.exec(command);
            cmd.join(30, TimeUnit.SECONDS);
            return new String(cmd.getInputStream().readAllBytes());
        }
    }

    private void start(String hostname, SshConfig ssh, String orchestratorUrl, int port,
                       String remoteDir, List<Path> extraJars) throws IOException {
        // Build classpath: agent fat jar first, then extra jars in lib/
        StringBuilder cp = new StringBuilder(remoteDir + "/agent.jar");
        for (Path extra : extraJars) {
            cp.append(":").append(remoteDir).append("/lib/").append(extra.getFileName());
        }
        // angela.rootDir must be an absolute path — set it to a subdirectory of the remote work dir
        String angelaRootDir = remoteDir + "/angela-root";
        // Propagate angela.* system properties from the local JVM to the remote agent
        // so that settings like angela.grid.provider=baton are honoured when the agent
        // spawns child processes (e.g. client JVMs via RemoteClientManager).
        StringBuilder jvmProps = new StringBuilder();
        for (String key : System.getProperties().stringPropertyNames()) {
            if (key.startsWith("angela.") && !key.equals("angela.rootDir")) {
                String val = System.getProperty(key);
                jvmProps.append(" -D").append(key).append("=").append(val);
            }
        }
        // Use -cp instead of -jar so the extra jars are visible to the classloader
        String cmd = "nohup java -cp " + cp
                + " -Dangela.rootDir=" + angelaRootDir
                + jvmProps
                + " io.baton.agent.AgentMain"
                + " --orchestrator " + orchestratorUrl
                + " --port " + port
                + " --hostname " + hostname
                + " > " + remoteDir + "/agent.log 2>&1 &";
        try (SSHClient client = newSshClient(hostname, ssh)) {
            exec(client, cmd);
        }
    }

    private void exec(SSHClient client, String command) throws IOException {
        try (Session session = client.startSession()) {
            System.out.println("Execute SSH command: " + command);
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
        System.out.println("Logging to remote host with username " + ssh.getUsername());
        client.authPublickey(ssh.getUsername(), ssh.getIdentityFile());
        return client;
    }
}
