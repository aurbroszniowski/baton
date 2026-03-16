# Baton

**Baton** is a lightweight distributed coordination framework for Java. It lets multiple JVMs share state through distributed primitives and execute serialized lambdas on remote workers, with no Ignite, Hazelcast, or ZooKeeper dependency.

It was built as a drop-in replacement for Apache Ignite inside the [Angela](https://github.com/Terracotta-OSS/angela) distributed test infrastructure.

**If you don't want to read the docs** and just want to start using it, check the [Quick start section](#quick-start).

**If you are a developer** and want to have guidance on the codebase check [README-developer.md](docs/README-developer.md)

---

## How it works

```
        - Execute lambda on remote servers
        - Use Distributed primitives (CyclicBarrier, AtomicReference, etc.)
                ▼
                │  JAVA
       ┌────────┴────────┐
       │ BATON JAVA API  │
 ┌──────                 ──────────────────┐
 │       Orchestrator JVM                  │
 │                                         │
 │  BatonFabric  ──►  HTTP Server          │
 │     │               (holds all state)   │
 │     │                                   │
 │  counter / boolean / reference          │
 │  barrier / queue                        │
 └──────────────┬──────────────────────────┘
                │  HTTP
        ┌───────┴────────┐
        ▼                ▼
  Agent JVM A      Agent JVM B
  (remote)         (remote)
```

All distributed state lives in the **orchestrator** JVM and is served over plain HTTP (JDK `HttpServer`, zero external deps). **Agents** are remote JVMs that receive serialized lambda jobs, execute them, and POST results back. Agents send heartbeats; once heartbeat grace is exceeded (15s by default), pending futures for that agent are failed.

**Three core principles:**
1. **Coordinator-centric state**: no distributed consensus, no quorum.
2. **Explicit class shipping**: lambda bytecode travels with the lambda; agents use a temporary classloader.
3. **HTTP everywhere**: the only transport is the JDK's built-in HTTP server.

---

## Remote lambda execution

The central idea is that you write an ordinary Java lambda in your orchestrator code and Baton runs it on a remote JVM transparently, with no RPC stubs, no code generation, and no shared classpath between machines.

**How it works step by step:**

1. **Capture**: when you call `fabric.executeAsync(node, () -> doWork())`, Baton serializes the lambda object using standard Java serialization.
2. **Class shipping**: a lambda compiled by javac is backed by a synthetic class (e.g. `MyClass$$Lambda$42`). That class, plus any other classes the lambda directly references that are not part of the JDK, are bundled as raw bytecode into a `ClassBundle` alongside the serialized lambda bytes.
3. **Transport**: the `ClassBundle` is sent to the target agent over a plain HTTP `POST /job` request.
4. **Remote loading**: the agent creates a fresh `BundleClassLoader` backed by the received bytecode map. It deserializes the lambda using this classloader so that all synthetic and captured classes resolve correctly.
5. **Execution**: the agent invokes `RemoteCallable.call()` or `RemoteRunnable.run()` on the deserialized lambda.
6. **Result**: the return value (or exception) is serialized and `POST`ed back to the orchestrator, which completes the `Future` returned to the caller.

**What can be captured:**

- Any `Serializable` value (primitives, strings, serializable POJOs).
- Distributed primitives (`DistributedCounter`, `DistributedReference`, etc.) obtained from the `Fabric` in HTTP mode are already serializable HTTP proxies. They can be captured and will call back to the orchestrator transparently from inside the remote lambda.

**What cannot be captured:**

- Non-serializable objects (e.g. open file handles, raw threads).
- Classes whose bytecode is not visible to the orchestrator at dispatch time (e.g. classes loaded by a custom classloader at runtime).

---

## Distributed primitives

Distributed primitives are named, shared variables whose state lives exclusively in the orchestrator JVM. Any node (local or remote) that holds a reference to a primitive talks to the orchestrator over HTTP to read or update it. There is no peer-to-peer synchronization and no consensus protocol; the orchestrator is the single source of truth.

| Primitive | Factory method | Description |
|---|---|---|
| `DistributedCounter` | `fabric.counter(name, initial)` | 64-bit integer with atomic increment, CAS, and get-and-set |
| `DistributedBoolean` | `fabric.bool(name, initial)` | Boolean flag with CAS |
| `DistributedReference<T>` | `fabric.reference(name, initial)` | Holds any `Serializable` value, with CAS |
| `DistributedBarrier` | `fabric.barrier(name, parties)` | Cyclic barrier: blocks until all `parties` have called `await()`, then resets |
| `DistributedQueue<T>` | `fabric.queue(name)` | FIFO blocking queue: `put` / `take` / `poll` with timeout |

Primitives with the same name share the same underlying state, any two calls to `fabric.counter("hits", 0L)` return handles to the same counter, whether they come from the same thread, different threads, or different JVMs connected to the same orchestrator.

In local mode (`FabricFactory.create(-1)`) all state is in-process with no HTTP overhead, which makes primitives suitable for multi-threaded coordination within a single JVM as well.

---

## Modules

| Module | Artifact | Purpose |
|---|---|---|
| `baton-api` | `io.github.aurbroszniowski.baton:baton-api` | Public interfaces: `Fabric`, `NodeId`, distributed primitive types |
| `baton-core` | `io.github.aurbroszniowski.baton:baton-core` | Orchestrator, HTTP server, in-memory state, HTTP proxy primitives, SSH deployment |
| `baton-agent` | *(not published)* | Standalone fat JAR embedded inside `baton-core` and deployed automatically |

---

## Quick start

### Add the dependency

**Gradle:**
```groovy
repositories { mavenLocal() }

dependencies {
    implementation 'io.github.aurbroszniowski.baton:baton-core:1.0.0'
}
```

**Maven:**
```xml
<dependency>
  <groupId>io.github.aurbroszniowski.baton</groupId>
  <artifactId>baton-core</artifactId>
  <version>1.0.0</version>
</dependency>
```

### Distributed primitives

```java
// port 0 -> OS picks a free port; use -1 for a pure in-process fabric (no HTTP server)
try (Fabric fabric = FabricFactory.create(0)) {

    // Counter
    DistributedCounter hits = fabric.counter("hits", 0L);
    hits.incrementAndGet();                  // -> 1
    hits.compareAndSet(1L, 42L);            // -> true

    // Boolean
    DistributedBoolean ready = fabric.bool("ready", false);
    ready.set(true);

    // Reference (any Serializable value)
    DistributedReference<String> label = fabric.reference("label", "v1");
    label.compareAndSet("v1", "v2");

    // Barrier: synchronizes N threads or jobs before any proceeds
    DistributedBarrier gate = fabric.barrier("gate", 2);

    // Queue
    DistributedQueue<String> tasks = fabric.queue("tasks");
    tasks.put("work-item");
    String item = tasks.take();             // blocks until available
}
```

### Execute jobs on worker nodes

```java
try (Fabric fabric = FabricFactory.create(0)) {

    // Register a local worker (same JVM, useful for testing)
    NodeId worker = fabric.connectLocal();

    // Runnable: fire and wait
    DistributedCounter counter = fabric.counter("n", 0L);
    fabric.executeAsync(worker, () -> counter.incrementAndGet()).get();
    System.out.println(counter.get()); // 1

    // Callable: returns a value
    Future<String> f = fabric.executeAsync(worker, () -> "hello from worker");
    System.out.println(f.get()); // "hello from worker"

    // Exception propagates as ExecutionException
    Future<Void> bad = fabric.executeAsync(worker, () -> { throw new RuntimeException("boom"); });
    try {
        bad.get();
    } catch (ExecutionException e) {
        System.out.println(e.getCause().getMessage()); // "boom"
    }
}
```

### Barrier example: synchronize two concurrent jobs

```java
try (Fabric fabric = FabricFactory.create(0)) {
    NodeId w1 = fabric.connectLocal();
    NodeId w2 = fabric.connectLocal();

    DistributedBarrier gate    = fabric.barrier("gate", 2);
    DistributedCounter counter = fabric.counter("count", 0L);

    Future<Void> f1 = fabric.executeAsync(w1, () -> {
        gate.await();             // waits for both workers
        counter.incrementAndGet();
    });
    Future<Void> f2 = fabric.executeAsync(w2, () -> {
        gate.await();
        counter.incrementAndGet();
    });

    f1.get(); f2.get();
    System.out.println(counter.get()); // 2
}
```

---

## Documentation

- [README-developer.md](docs/README-developer.md): Guidance for contributors
- [Getting Started](docs/getting-started.md): installation, first fabric, running tests
- [Distributed Primitives](docs/primitives.md): full API for counter, boolean, reference, queue, barrier
- [Job Execution](docs/jobs.md): local and remote lambda dispatch, exception handling
- [SSH Deployment](docs/deployment.md): deploying agents on remote hosts
