# Baton Developer Guide

This document is for an experienced Java developer who needs to maintain Baton without prior familiarity with the codebase.

Baton is intentionally small. The architecture is built around one strong constraint:

1. The orchestrator JVM is the single source of truth.
2. Agents are stateless executors plus local file endpoints.
3. All cross-process coordination is plain HTTP plus Java serialization.

If you keep those three facts in mind, most of the implementation reads naturally.

## 1. Start with the module split

Read the project as three modules with very different responsibilities:

1. `baton-api`
   Defines the public API and wire-level DTOs that must be visible on both sides:
   `Fabric`, primitive interfaces, `NodeId`, `RemoteRunnable`, `RemoteCallable`, `ClassBundle`, `AgentLauncher`, `FabricFactory`.

2. `baton-core`
   Contains the real implementation:
   orchestrator runtime, in-memory state stores, HTTP server, HTTP proxy objects, agent runtime helpers, deployment code, and `ServiceLoader` providers.

3. `baton-agent`
   Contains only the standalone entry point used to run a remote agent process. The actual runtime logic it uses mostly lives in `baton-core`.

The most important design decision here is that `ClassBundle` lives in `baton-api`, not in `baton-core`. That avoids a circular dependency between the code that creates bundles and the code that consumes them.

## 2. Understand the two runtime topologies

Baton has two operating modes that share one API but differ internally.

### Local-only mode

Created via `FabricFactory.create()` which delegates to `create(-1)`.

In this mode:

1. No HTTP server is started.
2. Distributed primitives are backed directly by in-memory stores.
3. `connectLocal()` creates logical nodes that execute in a local thread pool.
4. This mode is mainly for fast unit tests and same-JVM coordination.

### Orchestrator mode

Created via `FabricFactory.create(0)` or `create(port)`.

In this mode:

1. `BatonFabric` starts `BatonServer`.
2. Primitive state is still in-memory in the orchestrator.
3. The returned primitive handles are HTTP proxies, not direct in-memory objects.
4. Remote agents register over HTTP, receive jobs over HTTP, and call back to the orchestrator over HTTP.

That last point matters: even though the orchestrator owns the state, in HTTP mode the caller usually interacts with that state through serializable proxy objects.

## 3. Boot sequence and object graph

The normal boot chain is:

1. `FabricFactory.create(...)`
2. `ServiceLoader` lookup of `FabricProvider`
3. `BatonFabricProvider`
4. `BatonFabric`

`BatonFabric` is the real composition root. It creates and wires:

1. `PrimitivesStore`
2. `BarrierCoordinator`
3. `QueueStore`
4. `AgentRegistry`
5. `JobDispatcher`
6. Local worker thread pool
7. Optionally `BatonServer`

It also wires the failure path:

1. `AgentRegistry` declares an agent dead after heartbeat grace is exceeded.
2. Its death listener calls `JobDispatcher.failAgent(...)`.
3. All pending futures for that agent complete exceptionally.

When trying to understand a behavior, `BatonFabric` is the best first class to open because it exposes almost every subsystem and shows how they fit together.

## 4. State ownership model

All distributed state lives in the orchestrator JVM. There is no replication, no consensus, and no peer-to-peer state exchange.

Concretely:

1. Counters, booleans, and references live in `PrimitivesStore`.
2. Barriers live in `BarrierCoordinator`.
3. Queues live in `QueueStore`.
4. Agents never own authoritative primitive state.

This dramatically simplifies reasoning:

1. Correctness is mostly local to the orchestrator.
2. Remote code is transport plus serialization logic.
3. Split-brain and quorum problems do not exist because Baton does not try to solve them.

The tradeoff is equally explicit: if the orchestrator dies, Baton loses the cluster state.

## 5. Primitive implementations: local objects vs HTTP proxies

Every primitive factory method in `BatonFabric` follows the same pattern:

1. Ensure the named primitive exists in the orchestrator store.
2. If running in HTTP mode, return an `Http*Proxy`.
3. Otherwise return the local in-process implementation.

That means `fabric.counter("x", 0)` does not always return the same concrete type:

1. Local mode: `PrimitivesStore.LocalCounter`
2. HTTP mode: `HttpCounterProxy`

This is central to the design because the HTTP proxy is serializable and can be captured inside remote lambdas. Once deserialized on an agent, it still knows the orchestrator base URL and continues to mutate orchestrator-owned state remotely.

For maintainers, the proxy classes are small and worth reading in full:

1. `HttpCounterProxy`
2. `HttpBooleanProxy`
3. `HttpReferenceProxy`
4. `HttpBarrierProxy`
5. `HttpQueueProxy`

They are effectively the client-side transport layer for the primitive APIs.

## 6. Remote execution path, end to end

The most important implementation flow in the project is remote lambda execution.

### Step 1: API call

The caller invokes:

```java
Future<R> f = fabric.executeAsync(node, job);
```

`BatonFabric` chooses one of two paths:

1. Local node: execute directly in `localPool`
2. Remote node: delegate to `JobDispatcher`

### Step 2: Build a `ClassBundle`

`JobDispatcher` creates a unique `jobId` and then serializes the lambda using `ClassCollector`.

`ClassCollector` is an `ObjectOutputStream` subclass that intercepts class annotations while serialization happens. For every non-bootstrap class it sees, it tries to collect the corresponding `.class` bytes into a map.

The output is:

1. `bundle.lambda`
   Serialized lambda instance bytes
2. `bundle.classes`
   Class name to bytecode map for the non-JDK classes the lambda needs

This is the core trick that allows Baton to execute lambdas remotely without code generation or a shared deployment artifact for the synthetic lambda classes.

### Step 3: Dispatch to the agent

`JobDispatcher` serializes:

1. `jobId`
2. `ClassBundle`

and sends them to:

```text
POST /job
```

on the agent.

It also stores a `CompletableFuture` in `pending` keyed by `jobId`, and tracks `jobId` by `NodeId` so it can fail everything if the agent dies.

### Step 4: Agent receives and deserializes

The remote process runs `io.baton.agent.AgentMain`, which starts an `AgentServer`.

`AgentServer` accepts the payload at `POST /job` and forwards it asynchronously to `JobRunner`.

`JobRunner`:

1. Reads `jobId`
2. Reads `ClassBundle`
3. Builds a `BundleClassLoader`
4. Deserializes the lambda with an `ObjectInputStream` whose `resolveClass` first asks that classloader

This is the second half of the core trick. `ClassCollector` gathered bytecode on the orchestrator side; `BundleClassLoader` makes that bytecode resolvable on the agent side.

### Step 5: Execute

`JobRunner` then checks whether the deserialized object is:

1. `RemoteCallable`
2. `RemoteRunnable`

and invokes it.

### Step 6: Return result or exception

The agent serializes either:

1. the return value, or
2. the thrown exception

and POSTs it back to:

```text
POST /agent/result/{jobId}?exception=true|false
```

### Step 7: Complete the future

`BatonServer` routes that callback to `JobDispatcher.completeJob(...)`, which resolves the original future.

That is the full remote execution loop.

## 7. Why the lambda shipping works

If you are maintaining Baton, you need to understand exactly what is and is not guaranteed by the lambda shipping mechanism.

### What Baton assumes

1. The lambda object is Java-serializable.
2. Captured state is serializable.
3. Non-JDK classes needed by the lambda can be located as `.class` resources from the orchestrator-side classloader.

### What Baton ships

1. The serialized lambda instance
2. The bytecode for encountered non-JDK classes
3. Superclasses, interfaces, and enclosing classes seen via `ClassCollector.collectTransitively(...)`

### What Baton actually scans

It does not scan the whole JVM, the whole classpath, or every loaded class.

The discovery mechanism is much narrower:

1. `JobDispatcher` serializes the lambda with `ClassCollector`.
2. `ClassCollector` extends `ObjectOutputStream`.
3. During Java serialization, `annotateClass(Class<?> cl)` is invoked for each class descriptor written into the stream.
4. For each such class, Baton calls `collectTransitively(cl)`.

`collectTransitively(...)` then includes only a limited structural closure:

1. the class itself,
2. its superclass chain,
3. its directly implemented interfaces,
4. its enclosing class.

For each of those classes, Baton tries to load bytecode from the originating classloader using the `.class` resource path. If the bytecode resource is not accessible, the class is skipped.

This means Baton is not doing full dependency analysis. It does not inspect method bodies, it does not recursively walk arbitrary referenced types from bytecode, and it does not attempt a complete semantic closure of every class that might be used later at runtime.

### What Baton does not try to solve

1. Arbitrary custom classloader ecosystems
2. Dynamic bytecode generated without a retrievable `.class` resource
3. General distributed object graphs with open resources or non-serializable dependencies

### Why this is usually enough

For ordinary Java lambdas, this limited discovery is usually sufficient because:

1. the lambda's synthetic implementation class is part of the serialized object graph,
2. captured objects are serialized too, so their classes are also encountered,
3. the superclass/interface/enclosing-class closure usually covers the structural types needed to deserialize the lambda correctly.

That is enough for Baton to reconstruct the lambda object on the agent side in the common case.

### Where it can still fail

It can miss classes that are only needed later during execution and were not part of the serialized object graph or the small structural closure above.

Typical edge cases are:

1. classes loaded reflectively at runtime,
2. classes produced dynamically with no retrievable `.class` resource,
3. dependencies that only appear in method bodies and are never encountered during serialization,
4. unusual custom classloader setups where the orchestrator can serialize a class but cannot expose its bytecode as a resource.

When debugging remote execution, start with:

1. Did serialization of the lambda itself succeed?
2. Did `ClassCollector` gather the expected classes?
3. Can `BundleClassLoader` resolve them on the agent?
4. Is the failure really in transport, or is it a regular application exception thrown by the job?

## 8. HTTP surface area

The project intentionally uses a very small HTTP surface based on the JDK `HttpServer` and `HttpURLConnection`.

### Orchestrator endpoints

Defined in `BatonServer`:

1. `/agent/register`
2. `/agent/heartbeat`
3. `/agent/result/{jobId}`
4. `/primitive/counter/...`
5. `/primitive/boolean/...`
6. `/primitive/reference/...`
7. `/barrier/...`
8. `/transfer/...`

### Agent endpoints

Defined in `AgentServer`:

1. `/job`
2. `/health`
3. `/shutdown`
4. `/files`

This is deliberately not a generic RPC layer. Each endpoint is tied closely to one Baton concept.

## 9. Primitive internals

### Counters and booleans

Stored as `AtomicLong` values inside `PrimitivesStore`.

Booleans are represented as `0` or `1` in `AtomicLong`. This keeps implementation uniform and makes CAS simple.

### References

Stored as `AtomicReference<Object>`.

One subtle but important choice: reference CAS uses `Objects.equals(...)` semantics rather than `AtomicReference.compareAndSet` reference identity semantics. That matters because once values cross the serialization boundary, object identity is not preserved.

### Barriers

Implemented by `BarrierCoordinator` with a per-barrier monitor object, `generation`, and `arrived` count.

Key behavior:

1. It is cyclic, like `CyclicBarrier`.
2. HTTP callers long-poll the orchestrator thread handling the request.
3. Timeout messages include arrived-count detail.
4. `HttpBarrierProxy` tracks a local generation counter so the caller participates in the correct cycle.

If you need to change barrier behavior, read both sides:

1. `BarrierCoordinator`
2. `HttpBarrierProxy`

They form one protocol, not two independent implementations.

### Queues

Implemented by `QueueStore` using `LinkedBlockingQueue`.

In HTTP mode, `HttpQueueProxy` serializes items to the orchestrator and deserializes them on retrieval. Queue operations are therefore transport operations, not replicated in-memory handles.

## 10. Agent lifecycle and failure handling

The remote agent is intentionally simple:

1. Start HTTP server
2. Register with orchestrator
3. Start periodic heartbeat
4. Accept jobs
5. Stop on `/shutdown`

Heartbeat behavior is owned by `AgentRegistry`.

Current policy:

1. Heartbeat interval is 5 seconds
2. Dead after 3 missed heartbeats
3. Effective grace period is 15 seconds

Once an agent is declared dead, Baton does not attempt recovery of in-flight work. It fails the corresponding futures. This is the correct mental model: Baton provides dispatch and shared coordination primitives, not durable distributed job recovery.

## 11. File transfer path

File transfer was added as a pragmatic capability, not as a separate subsystem.

The path is:

1. `Fabric.upload(...)` or `Fabric.download(...)`
2. HTTP call directly to the agent `/files?path=...`
3. Agent reads or writes the local filesystem

Implications:

1. Files are transferred through the agent endpoint, not through the orchestrator.
2. Path expansion supports leading `~`.
3. Directory upload is implemented recursively in `BatonFabric`.
4. There is very little abstraction here; it is straightforward code and easy to extend.

## 12. Deployment path

Remote deployment is behind the `AgentLauncher` SPI.

The default implementation is:

1. `SshAgentLauncher`
2. which uses `AgentDeployer`
3. which uses `sshj`

The deployment flow is:

1. Resolve the agent fat JAR
2. Upload it over SCP to `~/baton/agent.jar`
3. Start it with `nohup java -jar ...`
4. Poll `/health`
5. Let the orchestrator discover the final registered `NodeId`

One useful packaging detail:

1. `baton-agent` builds a fat JAR
2. `baton-core` embeds that JAR as a resource during `processResources`
3. `SshAgentLauncher` extracts it to a temp file unless `-Dbaton.agent.jar=...` overrides it

This is why consumers only need `baton-core` even though a separate agent process exists.

## 13. Concurrency model

Baton uses only a few concurrency primitives, which keeps debugging tractable.

### Orchestrator side

1. Local jobs run on a cached thread pool in `BatonFabric`
2. HTTP requests run on a cached thread pool in `BatonServer`
3. Heartbeat monitoring runs on a single scheduled executor in `AgentRegistry`
4. Pending remote jobs are tracked with `CompletableFuture` plus concurrent maps in `JobDispatcher`

### Agent side

1. HTTP requests run on a cached thread pool in `AgentServer`
2. Jobs run asynchronously on a cached thread pool in `JobRunner`
3. Heartbeats run on a single scheduled executor in `HeartbeatReporter`

There is no reactive framework, message broker, or event loop to learn. Most behavior is plain blocking Java with explicit threads and concurrent maps.

## 14. Serialization boundaries you must respect

The hardest class of bugs in Baton will usually be serialization and classpath issues, not algorithmic complexity.

Be careful about:

1. Capturing non-serializable objects in remote lambdas
2. Returning non-serializable values from `RemoteCallable`
3. Assuming object identity survives round-trips
4. Adding new primitive or transport types without making their proxies serializable
5. Introducing dependencies on libraries not present in the agent fat JAR

If you add a new remotely usable primitive, ask two questions:

1. What is the authoritative orchestrator-side state representation?
2. What serializable proxy object will an agent capture and use remotely?

That pattern is repeated throughout the framework.

## 15. Service loading and packaging seams

Two extension seams are based on `ServiceLoader`:

1. `FabricFactory` loads `FabricProvider`
2. `BatonFabric.deployAndConnect(...)` loads `AgentLauncher`

The resource registrations live under `baton-core/src/main/resources/META-INF/services`.

This means:

1. `baton-core` is the runtime implementation module
2. Alternative implementations can be dropped in without changing `baton-api`
3. Startup failures around missing providers usually come from classpath or packaging issues, not from runtime logic

## 16. How to read the code efficiently

If you are onboarding to maintain Baton, this is the shortest useful reading order:

1. `baton-api/Fabric.java`
   Understand the public surface first.

2. `baton-core/BatonFabric.java`
   See the composition root and dispatch decisions.

3. `baton-core/BatonServer.java`
   Understand the orchestrator HTTP protocol.

4. `baton-core/JobDispatcher.java`
   Understand how remote jobs are created and tracked.

5. `baton-core/ClassCollector.java` and `baton-core/BundleClassLoader.java`
   Understand the lambda shipping mechanism.

6. `baton-core/JobRunner.java`
   Understand remote execution on the agent side.

7. `baton-core/PrimitivesStore.java`, `BarrierCoordinator.java`, `QueueStore.java`
   Understand actual shared-state implementations.

8. `baton-core/Http*Proxy`
   Understand the client side of remote primitive access.

9. `baton-core/AgentRegistry.java`
   Understand liveness and failure semantics.

10. `baton-core/deployer/*`
   Understand deployment only after the core runtime is clear.

## 17. Tests that explain the system best

The tests are small and map cleanly to architecture layers.

1. `ClassBundleTest`
   Best starting point for understanding lambda shipping in isolation.

2. `FabricTest`
   Best starting point for local mode and primitive semantics.

3. `HttpTransportIT`
   Best starting point for orchestrator HTTP-backed primitives.

4. `TwoProcessIT`
   Best starting point for the full real-process story:
   registration, heartbeats, remote execution, file transfer, and shutdown.

If you need confidence after changing a subsystem, these tests tell you which layer you broke.

## 18. Common maintenance tasks and where to change code

### Add a new primitive

You will usually need to touch:

1. `baton-api` interface
2. Orchestrator-side store/coordinator
3. HTTP server routes in `BatonServer`
4. Serializable HTTP proxy
5. Factory method in `BatonFabric`
6. Tests in local mode and HTTP mode

### Change remote execution behavior

You will usually need to touch:

1. `JobDispatcher`
2. `ClassCollector` and possibly `BundleClassLoader`
3. `AgentServer`
4. `JobRunner`
5. Result callback handling in `BatonServer`

### Change agent liveness policy

You will usually need to touch:

1. `HeartbeatReporter`
2. `AgentRegistry`
3. Integration tests that assume the current timing

### Change deployment behavior

You will usually need to touch:

1. `AgentLauncher`
2. `SshAgentLauncher`
3. `AgentDeployer`
4. Potentially build packaging for the embedded agent JAR

## 19. Current architectural limits

A maintainer should be aware of the deliberate limits of the current design.

1. No durable state
2. No distributed consensus or failover
3. No retry or replay for in-flight remote jobs
4. Java serialization is a hard dependency of the protocol
5. Barrier and queue operations rely on blocking HTTP requests
6. Deployment currently assumes SSH reachability and a fixed remote agent port
7. SSH host verification is permissive in the default deployer

These are not accidental bugs. They are design boundaries that keep the framework small.

## 20. The mental model to keep while maintaining Baton

If you need one sentence that explains the architecture, use this:

Baton is a coordinator-centric Java framework where the orchestrator owns all shared state, agents only execute shipped lambdas, and HTTP plus Java serialization is the entire distributed protocol.

That mental model explains:

1. why the codebase is small,
2. why the proxy objects matter so much,
3. why `ClassBundle` and `BundleClassLoader` are central,
4. why failure handling is mostly about agent death and pending futures,
5. and why most feature work ends up being "add orchestrator state + add serializable proxy + add HTTP route".
