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

import java.nio.file.Path;

/**
 * {@link AgentLauncher} implementation that uses SSH/SCP to deploy the agent JAR
 * to a remote host and start it.
 *
 * <p>The path to the fat JAR must be provided via the system property
 * {@code baton.agent.jar}, e.g.:
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
        String jarPath = System.getProperty("baton.agent.jar");
        if (jarPath == null) {
            throw new IllegalStateException(
                    "System property 'baton.agent.jar' must point to the baton-agent fat JAR");
        }
        AgentDeployer deployer = new AgentDeployer(Path.of(jarPath));
        return deployer.deploy(hostname, ssh, orchestratorUrl);
    }
}
