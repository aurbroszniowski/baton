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

import io.baton.AgentEndpoint;
import io.baton.DeploymentOptions;
import io.baton.NodeId;
import io.baton.SshConfig;
import io.baton.transport.HttpTransport;
import io.baton.transport.Serialization;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** SSH/SCP process startup. The orchestrator owns registration and liveness. */
public class AgentDeployer {
    private final Path agentJar;

    public AgentDeployer(Path agentJar) { this.agentJar = agentJar; }

    /** Compatibility entry point: start and wait for the agent's actual identity. */
    public NodeId deploy(String hostname, SshConfig ssh, String orchestratorUrl)
            throws IOException, InterruptedException, TimeoutException {
        return deploy(hostname, ssh, orchestratorUrl, LegacyDeploymentOptions.fromCurrentProcess());
    }

    public NodeId deploy(String hostname, SshConfig ssh, String orchestratorUrl, DeploymentOptions options)
            throws IOException, InterruptedException, TimeoutException {
        AgentEndpoint endpoint = start(hostname, ssh, orchestratorUrl, options);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try {
                byte[] body = HttpTransport.getBytes("http://" + endpoint.getHostname() + ":"
                        + endpoint.getPort() + "/identity", 1_000);
                return (NodeId) Serialization.read(body);
            } catch (IOException | ClassNotFoundException e) {
                Thread.sleep(100);
            }
        }
        throw new TimeoutException("Agent did not become ready: " + hostname + ":" + endpoint.getPort());
    }

    /** Start the process and return its address without polling health or inventing a node identity. */
    public AgentEndpoint start(String hostname, SshConfig ssh, String orchestratorUrl, DeploymentOptions options)
            throws IOException, InterruptedException {
        int port = 8700;
        stopIfRunning(hostname, port);
        try (SSHClient client = newSshClient(hostname, ssh)) {
            String home = execOutput(client, "echo $HOME").trim();
            String directory = resolveWorkingDirectory(home, options.getWorkingDirectory());
            String callbackUrl = orchestratorUrl.replaceFirst("http://[^:/]+",
                    "http://" + client.getLocalAddress().getHostAddress());
            exec(client, "mkdir -p " + quote(directory + "/lib"));
            client.newSCPFileTransfer().upload(new FileSystemFile(agentJar.toFile()), directory + "/agent.jar");
            for (Path jar : options.getApplicationJars()) {
                client.newSCPFileTransfer().upload(new FileSystemFile(jar.toFile()), directory + "/lib/" + jar.getFileName());
            }
            exec(client, startCommand(hostname, callbackUrl, port, directory, options));
        }
        return new AgentEndpoint(hostname, port);
    }

    public void stop(NodeId node) throws IOException {
        HttpTransport.post("http://" + node.getHostname() + ":" + node.getPort() + "/shutdown");
    }

    private void stopIfRunning(String hostname, int port) throws InterruptedException {
        try {
            HttpTransport.post("http://" + hostname + ":" + port + "/shutdown", new byte[0],
                    "application/octet-stream", 2_000);
            Thread.sleep(2_000);
        } catch (IOException ignored) {
            // Nothing is listening on the fixed agent port.
        }
    }

    static String resolveWorkingDirectory(String home, String configured) {
        if (configured.startsWith("/")) return configured;
        if (configured.equals("~")) return home;
        return home + "/" + (configured.startsWith("~/") ? configured.substring(2) : configured);
    }

    static String startCommand(String hostname, String orchestratorUrl, int port,
                               String directory, DeploymentOptions options) {
        StringBuilder classpath = new StringBuilder(directory + "/agent.jar");
        for (Path jar : options.getApplicationJars()) classpath.append(":").append(directory).append("/lib/").append(jar.getFileName());
        StringBuilder command = new StringBuilder("cd ").append(quote(directory))
                .append(" && nohup java -cp ").append(quote(classpath.toString()))
                .append(" ").append(quote("-Dbaton.agent.workDir=" + directory));
        for (Map.Entry<String, String> property : new TreeMap<>(options.getJvmProperties()).entrySet()) {
            command.append(" ").append(quote("-D" + property.getKey() + "="
                    + property.getValue().replace("${workDir}", directory)));
        }
        return command.append(" io.baton.agent.AgentMain --orchestrator ").append(quote(orchestratorUrl))
                .append(" --port ").append(port).append(" --hostname ").append(quote(hostname))
                .append(" > ").append(quote(directory + "/agent.log")).append(" 2>&1 < /dev/null &").toString();
    }

    private static String quote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    private String execOutput(SSHClient client, String command) throws IOException {
        try (Session session = client.startSession()) {
            Session.Command result = session.exec(command);
            result.join(30, TimeUnit.SECONDS);
            if (result.getExitStatus() == null || result.getExitStatus() != 0) {
                throw new IOException("Remote command failed: " + command);
            }
            return new String(result.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private void exec(SSHClient client, String command) throws IOException { execOutput(client, command); }

    private SSHClient newSshClient(String hostname, SshConfig ssh) throws IOException {
        SSHClient client = new SSHClient();
        client.addHostKeyVerifier(new PromiscuousVerifier());
        try {
            client.connect(hostname, ssh.getPort());
            client.authPublickey(ssh.getUsername(), ssh.getIdentityFile());
            return client;
        } catch (IOException e) {
            client.close();
            throw e;
        }
    }
}
