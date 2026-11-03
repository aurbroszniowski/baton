# Baton developer guide

Baton has one authoritative orchestrator and stateless worker agents. All
cross-process coordination uses JDK HTTP and Java serialization. There is no
replication, durable job recovery, or distributed consensus.

## Modules and packaging

| Module | Responsibility |
|---|---|
| `baton-api` | Public interfaces, serializable job types, `ClassBundle`, node identity, deployment inputs, and extension hooks |
| `baton-runtime` | The shared agent implementation, classloader, log relay, HTTP transport, serialization, and file helpers; depends only on `baton-api` and the JDK |
| `baton-core` | Orchestrator, state stores, primitive proxies, job dispatch, registration/liveness, SSH deployment, and the embedded-agent compatibility adapter |
| `baton-agent` | The standalone `AgentMain` entry point and fat-JAR assembly |

The production dependency graph is:

```text
baton-api <- baton-runtime <- baton-agent
                         <- baton-core
```

`baton-agent` bundles the API and runtime into a standalone fat JAR. Core embeds
that finished JAR as a resource, without depending on the agent's implementation
classes. Both entry points use the same `AgentRuntime`; agent code has no
dependency on the orchestrator or SSH libraries. Consumers still declare only
`baton-core`; the API and runtime are transitive dependencies.

## Orchestrator and local workers

`FabricFactory` discovers `BatonFabricProvider` with `ServiceLoader`.
`BatonFabric` is the orchestrator composition root. It owns:

- `PrimitivesStore`: counters, booleans, and references.
- `BarrierCoordinator`: cyclic barriers.
- `QueueStore`: blocking FIFO queues.
- `AgentRegistry`: remote registration and heartbeat expiry.
- `JobDispatcher`: pending remote jobs and their futures.
- A local job executor and a separate set of local worker identities.
- An optional `BatonServer` and a configurable remote log sink.

`FabricFactory.create()` or `create(-1)` starts local-only mode, with no HTTP
server. Primitive handles access the in-memory stores directly.
`create(0)` or `create(port)` starts HTTP mode and returns serializable HTTP
primitive proxies. HTTP startup failure aborts construction instead of silently
falling back to local-only mode.

`connectLocal()` creates a logical worker in this JVM. Its jobs run directly,
without serialization. Local workers never enter the remote heartbeat registry.
`getConnectedNodes()` combines local workers with registered, live remote agents.

## One agent runtime, two startup adapters

`baton-runtime` contains the only implementations of `AgentServer`, `JobRunner`,
`HeartbeatReporter`, and `BundleClassLoader`.

`AgentRuntime` owns the agent lifecycle:

1. Bind its HTTP server, allowing port `0` for an embedded agent.
2. Build the actual `NodeId`, including the bound port and process ID.
3. Allow the startup adapter to run its initializers while jobs, health, and identity report unavailable.
4. On `start(orchestratorUrl)`, enable initialized job execution, publish registration,
   then start heartbeats and expose ready health and identity endpoints.
5. On shutdown, stop heartbeats, HTTP workers, and jobs, and release shutdown waiters.

Construction binds and establishes identity without contacting an orchestrator.
Startup failure closes resources already started. Shutdown is idempotent.
A latch records shutdown even if it happens before a caller begins waiting.

`AgentMain` adds process-specific behavior: stdout/stderr capture, asynchronous
log relay, child-process cleanup at JVM exit, and `RemoteInitializer` discovery.
It runs initializers before calling `start()`, announces readiness afterwards,
then waits on the runtime and closes it when finished. Job acceptance is enabled
just before registration is published so immediate registry-driven dispatch does
not race with the registration response. `/identity` remains unavailable until
registration succeeds. The brief pre-registration job-acceptance window also
allows a stale orchestrator that retained an old identity at the same endpoint
to dispatch. Under the trusted-peer assumption this is accepted; registration is
not an authorization boundary or a fence against stale senders.
`BatonAgentFabric` has no initializers: it installs its log relay before calling
`start()` and resets the relay to `NOOP` if startup fails.

`BatonAgentFabric` retains the existing `Fabric` interface for applications such
as Angela. It delegates lifecycle and execution to `AgentRuntime`, creates HTTP
primitive handles, and uses shared file helpers. Its execution and file methods
require its own node identity. Agent deployment and creation of sub-nodes remain
unsupported compatibility methods. New agent lifecycle code should use
`AgentRuntime` directly rather than implementing the orchestrator interface.

## Remote jobs and class shipping

The remote path is:

```text
Fabric.executeAsync
  -> JobDispatcher: collect bytecode, serialize job, record pending future
  -> agent POST /job
  -> JobRunner: deserialize using BundleClassLoader, invoke job
  -> orchestrator POST /agent/result/{jobId}?exception=true|false
  -> JobDispatcher: complete the original future
```

`ClassCollector` observes class descriptors during Java serialization. It gathers
retrievable non-JDK `.class` resources for encountered classes, their
superclasses, interfaces, enclosing classes, field types, and method/constructor
signature types (including array components). Serializable
lambdas use Java's `SerializedLambda` representation, which identifies the
capturing class; generated lambda class bytes need not exist as resources.

This is a structural scan, not full bytecode dependency analysis. Dependencies
used only in method bodies or reflection may need explicit application JARs on
the remote classpath. Dynamically generated classes without retrievable resources
and non-serializable captured objects remain outside the supported boundary.

`JobRunner` sets `RemoteExecutionContext` before invocation and clears it in a
`finally` block. Results and exceptions return through the same callback protocol.
Pending jobs live in one map; each entry owns its target node and future.
Agent expiry and disconnect fail that node's pending jobs. Fabric shutdown fails
all pending jobs and rejects further remote dispatch. Late callbacks do not
revive failed jobs. Jobs are not retried or replayed.

## Primitive state and transport

The authoritative primitive implementations are shared by local calls and HTTP
handlers. HTTP proxies carry only their endpoint, name, and required primitive
metadata, so they can be captured in serialized jobs.

Counters and booleans use `AtomicLong`. Reference CAS compares values with
`Objects.equals` and performs an atomic update loop: Java reference identity does
not survive serialization. Queues use `LinkedBlockingQueue`.

Barriers use a server-side generation and arrived count. `HttpBarrierProxy`
maintains a client generation and uses blocking HTTP requests. Read the proxy and
coordinator together before changing their protocol. Timeout messages include the
number of arrived parties. Queues also use blocking HTTP requests for reads.

`io.baton.transport.HttpTransport` supplies bounded ordinary requests and explicit
read timeouts for long polls. It closes streams and disconnects connections.
`Serialization` supplies common object encoding. `AgentFiles` supplies streamed
file copies, POSIX mode conversion, home-directory expansion, and path aliases.
The helpers are in the runtime so shipped primitive proxies can use them in the
standalone agent without requiring core on its classpath.

Primitive names and file paths are URL encoded. File upload and download stream
their content; upload also carries POSIX permission metadata where supported.
Directories are uploaded recursively. `~` expands to the current user's home.
An alias such as `data://result.txt` resolves through `-Dbaton.path.data=/root`.
The existing `angela-root://` convention remains available for compatibility.

## Registration, deployment, and failure

`AgentRegistry` tracks remote agents only. Agents send heartbeats every five
seconds. After the 15-second grace period, the registry removes the expired
identity and tells `JobDispatcher` to fail its pending jobs. An unknown or expired
identity cannot revive itself with a late heartbeat; it must register again.

For the standard SSH path:

1. `SshAgentLauncher.start()` resolves the embedded agent JAR and invokes the deployer.
2. `AgentDeployer.start()` uploads files, starts the process, and returns an `AgentEndpoint`.
3. `BatonFabric` waits on the registry for the real registered `NodeId`, or fails after 30 seconds.

An endpoint is an address, not a provisional node identity. Registration is the
only source of the identity returned by `deployAndConnect()`. Existing custom
`AgentLauncher` implementations remain usable through the default `start()`
adapter around their `launch()` method.

The registry records a generation for every registration, including repeated
registrations of exactly the same `NodeId`. Deployment snapshots that generation
before startup and waits for a newer matching registration, avoiding stale identities.

The older direct `AgentDeployer.deploy()` entry point remains available. It
starts the process and polls `/identity` for the agent's actual registered identity.
Normal Fabric deployment does not need that extra HTTP polling step.

Fabric shutdown shares a two-second budget for remote shutdown attempts, with
individual connect/read timeouts capped at 500 ms and the remaining budget.
Remaining identities and pending jobs are cleared even when requests fail or the
budget runs out. This avoids a ten-second wait per unresponsive agent.
Healthy agents later in the shutdown sequence can receive less than 500 ms or
no `/shutdown` request when earlier attempts consume the budget. Agents that do
not receive the request rely on heartbeat failure detection to shut themselves
down after the orchestrator stops.

`DeploymentOptions` explicitly supplies application JARs, JVM properties, and a
working directory. Property values may use `${workDir}` for the absolute remote
working directory. Relative directories resolve under the remote user's home.
Adapters should supply framework-specific inputs here. The original deployment
overload uses isolated `LegacyDeploymentOptions`, preserving classpath discovery
and Angela property propagation for existing callers.

The agent still uses fixed remote port `8700`. SSH host key verification remains
permissive. There is no automatic agent replacement or recovery of in-flight jobs.

## Logging and integration hooks

Logging is best effort and must not affect job success. `LogRelayClient` uses a
bounded non-blocking queue, batches records by metadata, and POSTs `/agent/logs`.
Records include node, job, source, label, and stream. Queue overflow and transport
failure may drop records. `LogCollector` forwards received records to `LogSink`;
`PrintingLogSink` prints structured lines and `FileLogSink` writes job log files.

`AgentMain` installs `SystemStreamCapture` to tee stdout/stderr. Embedded agents
retain explicit `BatonRuntime.emit(...)` support without changing process-wide
stdout/stderr. `BatonAgentRuntime.attachProcess(process, label)` relays a child
process's stdout/stderr using the current job context.

Frameworks can install logging bridges and static state through
`RemoteInitializer`, registered under `META-INF/services/io.baton.RemoteInitializer`.
Initializer exceptions are logged without aborting other initializers. Baton
provides `JulRelayHandler`; other logging framework adapters remain application-owned.

## Tests and maintenance

Run the suite with:

```bash
./gradlew test
```

- `FabricTest` covers local execution and primitive semantics.
- `HttpTransportIT` covers HTTP primitive behavior.
- `ClassBundleTest` isolates collection, deserialization, and execution.
- `TwoProcessIT` starts the packaged agent with `java -jar`, with no test or
  orchestrator classpath, and covers shipped classes, captured proxies, results,
  identity/context, heartbeat, files, permissions, and shutdown.
- `EmbeddedAgentTest` covers the same runtime through the compatibility adapter.
- Registration failure tests verify cleanup when startup cannot complete.
- `AgentReadinessIT` gates a deliberately slow service-loaded initializer in the
  packaged agent, then dispatches immediately on registration and verifies that
  both the first job and the compatibility identity path see initialized state.
- Registry and dispatcher tests cover expiry, registration timeout, job failure,
  and late results without waiting for the real heartbeat grace period.
- Logging tests cover capture, process relay, collectors, and sinks.

When adding a primitive, update its API, authoritative store, HTTP routes, proxy,
Fabric factories, and local/HTTP tests. When changing agent behavior, update the
shared runtime once and exercise both startup adapters. Keep packaged-agent
coverage: a child JVM using the test classpath can conceal missing runtime dependencies.
