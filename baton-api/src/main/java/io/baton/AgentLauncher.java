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
package io.baton;

/**
 * SPI for deploying a remote baton agent.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}.  The default
 * implementation in {@code baton-deployer} uses SSH/SCP ({@code sshj}) to upload and
 * start the agent JAR on a remote host.
 *
 * <pre>{@code
 * // baton-deployer registers:
 * //   META-INF/services/io.baton.AgentLauncher → io.baton.deployer.SshAgentLauncher
 * }</pre>
 */
public interface AgentLauncher {

    /**
     * Deploy and start the agent on {@code hostname}, then wait for it to register
     * with the orchestrator.
     *
     * @param hostname        target host for the agent
     * @param ssh             SSH credentials
     * @param orchestratorUrl HTTP base URL of the orchestrator, e.g. {@code http://host:9400}
     * @return the {@link NodeId} that the agent registered under
     * @throws Exception if deployment or startup fails
     */
    NodeId launch(String hostname, SshConfig ssh, String orchestratorUrl) throws Exception;
}
