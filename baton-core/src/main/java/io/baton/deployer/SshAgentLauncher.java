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

import io.baton.AgentLauncher;
import io.baton.NodeId;
import io.baton.SshConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * {@link AgentLauncher} implementation that uses SSH/SCP to deploy the agent JAR
 * to a remote host and start it.
 *
 * <p>The agent JAR is embedded inside {@code baton-core} and extracted to a temporary
 * file automatically. To use a custom build instead, set the system property
 * {@code baton.agent.jar} to the path of the fat JAR:
 * <pre>
 *   java -Dbaton.agent.jar=/path/to/baton-agent-fat.jar ...
 * </pre>
 *
 * <p>Registered as a {@link java.util.ServiceLoader} provider via
 * {@code META-INF/services/io.baton.AgentLauncher}.
 */
public class SshAgentLauncher implements AgentLauncher {

    @Override
    public NodeId launch(String hostname, SshConfig ssh, String orchestratorUrl) throws Exception {
        AgentDeployer deployer = new AgentDeployer(resolveAgentJar());
        return deployer.deploy(hostname, ssh, orchestratorUrl);
    }

    private static Path resolveAgentJar() throws IOException {
        String override = System.getProperty("baton.agent.jar");
        if (override != null) {
            return Path.of(override);
        }
        try (InputStream is = SshAgentLauncher.class.getResourceAsStream("baton-agent.jar")) {
            if (is == null) {
                throw new IllegalStateException(
                        "Embedded baton-agent.jar not found in classpath. " +
                        "Re-build baton-core, or set the 'baton.agent.jar' system property.");
            }
            Path tmp = Files.createTempFile("baton-agent-", ".jar");
            tmp.toFile().deleteOnExit();
            Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
            return tmp;
        }
    }
}
