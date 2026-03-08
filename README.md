# Baton

**Baton** is a lightweight distributed coordination framework for Java. It lets multiple JVMs share state through distributed primitives and execute serialised lambdas on remote workers — with no Ignite, Hazelcast, or ZooKeeper dependency.

It was built as a drop-in replacement for Apache Ignite inside the [Angela](https://github.com/Terracotta-OSS/angela) distributed test infrastructure.

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
 │  BatonFabric  ──►  HTTP Server (:8080)  │
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

All distributed state lives in the **orchestrator** JVM and is served over plain HTTP (JDK `HttpServer` — zero external deps). **Agents** are remote JVMs that receive serialised lambda jobs, execute them, and POST results back. Agents send heartbeats; once heartbeat grace is exceeded (15s by default), pending futures for that agent are failed.

**Three core principles:**
1. **Coordinator-centric state** — no distributed consensus, no quorum.
2. **Explicit class shipping** — lambda bytecode travels with the lambda; agents use a temporary classloader.
3. **HTTP everywhere** — the only transport is the JDK's built-in HTTP server.

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
// port 0 → OS picks a free port; use -1 for a pure in-process fabric (no HTTP server)
try (Fabric fabric = FabricFactory.create(0)) {

    // Counter
    DistributedCounter hits = fabric.counter("hits", 0L);
    hits.incrementAndGet();                  // → 1
    hits.compareAndSet(1L, 42L);            // → true

    // Boolean
    DistributedBoolean ready = fabric.bool("ready", false);
    ready.set(true);

    // Reference (any Serializable value)
    DistributedReference<String> label = fabric.reference("label", "v1");
    label.compareAndSet("v1", "v2");

    // Barrier — synchronises N threads or jobs before any proceeds
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

    // Runnable — fire and wait
    DistributedCounter counter = fabric.counter("n", 0L);
    fabric.executeAsync(worker, () -> counter.incrementAndGet()).get();
    System.out.println(counter.get()); // 1

    // Callable — returns a value
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

### Barrier example — synchronise two concurrent jobs

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

- [Getting Started](docs/getting-started.md) — installation, first fabric, running tests
- [Distributed Primitives](docs/primitives.md) — full API for counter, boolean, reference, queue, barrier
- [Job Execution](docs/jobs.md) — local and remote lambda dispatch, exception handling
- [SSH Deployment](docs/deployment.md) — deploying agents on remote hosts
