/*
 * Copyright Aurélien Broszniowski
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

import io.baton.AgentLogRelay;
import io.baton.BatonRuntime;
import io.baton.DistributedBarrier;
import io.baton.DistributedBoolean;
import io.baton.DistributedCounter;
import io.baton.DistributedQueue;
import io.baton.DistributedReference;
import io.baton.Fabric;
import io.baton.NodeId;
import io.baton.RemoteCallable;
import io.baton.RemoteExecutionContext;
import io.baton.RemoteRunnable;
import io.baton.SshConfig;
import io.baton.agent.AgentRuntime;
import io.baton.transport.AgentFiles;
import io.baton.transport.HttpTransport;
import io.baton.transport.Serialization;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Future;

/** Compatibility adapter exposing an embedded {@link AgentRuntime} as a {@link Fabric}. */
public class BatonAgentFabric implements Fabric {
    private final String orchestratorUrl;
    private final AgentRuntime runtime;

    public BatonAgentFabric(String orchestratorUrl, int agentPort, String agentName) throws IOException {
        this.orchestratorUrl = orchestratorUrl;
        String hostname;
        try { hostname = InetAddress.getLocalHost().getHostName(); }
        catch (UnknownHostException e) { hostname = "localhost"; }
        runtime = new AgentRuntime(agentPort, agentName, hostname);
        BatonRuntime.install((source, label, stream, line) -> {
            String job = RemoteExecutionContext.jobId();
            String tag = label != null && !"-".equals(label) ? label : "-";
            System.out.printf("[%s][%s][%s] %s%n", getLocalNodeId().toLogTag(),
                    PrintingLogSink.abbreviate(job != null ? job : "-"), tag, line);
        });
        try {
            runtime.start(orchestratorUrl);
        } catch (IOException | RuntimeException e) {
            BatonRuntime.install(AgentLogRelay.NOOP);
            throw e;
        }
    }

    @Override public NodeId getLocalNodeId() { return runtime.getNodeId(); }
    @Override public int getOrchestratorPort() { return -1; }
    @Override public String getOrchestratorUrl() { return orchestratorUrl; }

    @Override
    public NodeId deployAndConnect(String hostname, SshConfig ssh) {
        throw new UnsupportedOperationException("Agent cannot deploy other agents");
    }

    @Override
    public NodeId connectLocal() {
        throw new UnsupportedOperationException("Agent cannot create local sub-nodes");
    }

    @Override public Collection<NodeId> getConnectedNodes() { return Collections.emptyList(); }
    @Override public void disconnect(NodeId node) { /* Retained for Fabric compatibility. */ }

    @Override
    public Future<Void> executeAsync(NodeId node, RemoteRunnable job) {
        requireSelf(node);
        return runtime.execute(job);
    }

    @Override
    public <R> Future<R> executeAsync(NodeId node, RemoteCallable<R> job) {
        requireSelf(node);
        return runtime.execute(job);
    }

    @Override
    public DistributedCounter counter(String name, long initialValue) {
        initialize("/primitive/counter/" + HttpTransport.encode(name) + "/get?init=" + initialValue, new byte[0]);
        return new HttpCounterProxy(orchestratorUrl, name);
    }

    @Override
    public DistributedBoolean bool(String name, boolean initialValue) {
        initialize("/primitive/boolean/" + HttpTransport.encode(name) + "/get?init=" + initialValue, new byte[0]);
        return new HttpBooleanProxy(orchestratorUrl, name);
    }

    @Override
    public <T extends Serializable> DistributedReference<T> reference(String name, T initialValue) {
        try {
            initialize("/primitive/reference/" + HttpTransport.encode(name) + "/init", Serialization.write(initialValue));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize initial value for " + name, e);
        }
        return new HttpReferenceProxy<>(orchestratorUrl, name);
    }

    @Override
    public DistributedBarrier barrier(String name, int parties) {
        return new HttpBarrierProxy(orchestratorUrl, name, parties);
    }

    @Override
    public <T extends Serializable> DistributedQueue<T> queue(String name) {
        return new HttpQueueProxy<>(orchestratorUrl, name);
    }

    @Override
    public void upload(NodeId node, Path localPath, String remotePath) {
        requireSelf(node);
        try { AgentFiles.upload(localPath, remotePath); }
        catch (IOException e) { throw new RuntimeException("Local upload failed", e); }
    }

    @Override
    public void download(NodeId node, String remotePath, Path localDest) {
        requireSelf(node);
        try { AgentFiles.copy(AgentFiles.resolve(remotePath), localDest); }
        catch (IOException e) { throw new RuntimeException("Local download failed", e); }
    }

    @Override
    public void close() {
        runtime.close();
        BatonRuntime.install(AgentLogRelay.NOOP);
    }

    private void requireSelf(NodeId node) {
        if (!getLocalNodeId().equals(node)) throw new IllegalArgumentException("Agent can only execute on itself: " + node);
    }

    private void initialize(String path, byte[] body) {
        try { HttpTransport.post(orchestratorUrl + path, body); }
        catch (IOException e) { throw new IllegalStateException("Cannot initialize primitive: " + path, e); }
    }
}
