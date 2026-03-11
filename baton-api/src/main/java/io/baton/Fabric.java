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

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * The main entry point for Baton.
 *
 * <p>Manages the lifecycle of remote agent JVMs and provides access to
 * distributed primitives.  All state lives in the orchestrator (the JVM that
 * holds the {@code Fabric} instance); agents reach it via HTTP.
 *
 * <pre>{@code
 * try (Fabric fabric = FabricFactory.create()) {
 *     NodeId worker = fabric.deployAndConnect("server-a", SshConfig.of("user", "~/.ssh/id_rsa"));
 *     DistributedCounter counter = fabric.counter("jobs", 0L);
 *
 *     Future<Void> f = fabric.executeAsync(worker, () -> {
 *         long n = counter.incrementAndGet();
 *         System.out.println("Running job #" + n);
 *     });
 *     f.get();
 * }
 * }</pre>
 */
public interface Fabric extends AutoCloseable {

    // ── Node lifecycle ──────────────────────────────────────────────────────

    /** Returns the {@link NodeId} of the orchestrator itself. */
    NodeId getLocalNodeId();

    /** Returns the port the orchestrator HTTP server is listening on, or -1 if not in server mode. */
    int getOrchestratorPort();

    /** Returns the base URL of the orchestrator HTTP server (e.g. "http://host:port"), or null if not in server mode. */
    String getOrchestratorUrl();

    /** SSH into {@code hostname}, SCP the agent JAR, start the agent process, and wait for it to register. */
    NodeId deployAndConnect(String hostname, SshConfig ssh);

    /** Spawn a local agent in a child process on the same machine. */
    NodeId connectLocal();

    /** Returns all currently connected (alive) agent nodes. */
    Collection<NodeId> getConnectedNodes();

    /** Gracefully stops the agent and removes it from the registry. */
    void disconnect(NodeId node);

    // ── Remote execution ────────────────────────────────────────────────────

    Future<Void>  executeAsync(NodeId node, RemoteRunnable job);
    <R> Future<R> executeAsync(NodeId node, RemoteCallable<R> job);

    // ── Distributed primitives ──────────────────────────────────────────────

    DistributedCounter counter(String name, long initialValue);
    DistributedBoolean bool(String name, boolean initialValue);
    <T extends Serializable> DistributedReference<T> reference(String name, T initialValue);
    DistributedBarrier barrier(String name, int parties);
    <T extends Serializable> DistributedQueue<T> queue(String name);

    // ── File transfer ───────────────────────────────────────────────────────

    void upload(NodeId node, Path localPath, String remotePath);
    void download(NodeId node, String remotePath, Path localDest);

    @Override
    void close();
}
