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
package io.baton.angela;

import io.baton.Fabric;
import io.baton.NodeId;
import io.baton.SshConfig;
import org.terracotta.angela.agent.com.AgentExecutor;
import org.terracotta.angela.agent.com.AgentGroup;
import org.terracotta.angela.agent.com.AgentID;
import org.terracotta.angela.agent.com.Executor;
import org.terracotta.angela.agent.com.FileTransfer;
import org.terracotta.angela.agent.com.RemoteCallable;
import org.terracotta.angela.agent.com.RemoteRunnable;
import org.terracotta.angela.common.clientconfig.ClientId;
import org.terracotta.angela.common.cluster.Cluster;
import org.terracotta.angela.common.distribution.Distribution;
import org.terracotta.angela.common.topology.InstanceId;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Bridges Angela's {@link Executor} SPI to Baton's {@link Fabric}.
 */
class BatonExecutor implements Executor {

    private final Fabric fabric;

    BatonExecutor(Fabric fabric) {
        this.fabric = fabric;
    }

    @Override
    public AgentID getLocalAgentID() {
        return toAngela(fabric.getLocalNodeId());
    }

    @Override
    public Optional<AgentID> findAgentID(String hostname) {
        return fabric.getConnectedNodes().stream()
                .filter(n -> n.getHostname().equals(hostname))
                .findFirst()
                .map(BatonExecutor::toAngela);
    }

    @Override
    public Optional<AgentID> startRemoteAgent(String hostname) {
        try {
            SshConfig ssh = SshConfig.of(System.getProperty("user.name"), "~/.ssh/id_rsa");
            NodeId node = fabric.deployAndConnect(hostname, ssh);
            return Optional.of(toAngela(node));
        } catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
    }

    @Override
    public AgentGroup getGroup() {
        throw new UnsupportedOperationException("AgentGroup not yet implemented");
    }

    @Override
    public Cluster getCluster() {
        throw new UnsupportedOperationException("Cluster not yet implemented");
    }

    @Override
    public Cluster getCluster(ClientId clientId) {
        throw new UnsupportedOperationException("Cluster not yet implemented");
    }

    @Override
    public Future<Void> executeAsync(AgentID agentID, RemoteRunnable job) {
        return fabric.executeAsync(toNodeId(agentID), job::run);
    }

    @Override
    public <R> Future<R> executeAsync(AgentID agentID, RemoteCallable<R> job) {
        return fabric.executeAsync(toNodeId(agentID), job::call);
    }

    @Override
    public void uploadClientJars(AgentID agentID, InstanceId instanceId, List<Path> locations) {
        for (Path jar : locations) {
            fabric.upload(toNodeId(agentID), jar, "~/baton/jars/" + jar.getFileName());
        }
    }

    @Override
    public void uploadKit(AgentID agentID, InstanceId instanceId,
                          Distribution distribution, String kitInstallationName, Path kitInstallationPath) {
        fabric.upload(toNodeId(agentID), kitInstallationPath, "~/angela/kits/" + kitInstallationName);
    }

    @Override
    public BlockingQueue<FileTransfer> getFileTransferQueue(InstanceId instanceId) {
        // TODO Phase 5
        throw new UnsupportedOperationException("File transfer queue not yet implemented");
    }

    @Override
    public Optional<CompletableFuture<Void>> shutdown(AgentID agentID) {
        fabric.disconnect(toNodeId(agentID));
        return Optional.empty();
    }

    @Override
    public void close() {
        // fabric lifecycle is managed by BatonGridProvider
    }

    // ── Conversions ───────────────────────────────────────────────────────────

    static NodeId toNodeId(AgentID id) {
        return new NodeId(id.getName(), id.getHostName(), id.getPort(), id.getPid());
    }

    static AgentID toAngela(NodeId n) {
        return new AgentID(n.getName(), n.getHostname(), n.getPort(), n.getPid());
    }
}
