# Job Execution

Baton lets you execute lambdas on any connected node — local or remote. The orchestrator serialises the lambda together with the class bytes it needs, ships everything to the target agent over HTTP, and resolves the `Future` when the agent POSTs the result back.

---

## Local workers

`fabric.connectLocal()` adds a worker that runs in the same JVM using a cached thread pool. No serialisation occurs — useful for testing or for parallelising CPU-bound work without extra processes.

```java
try (Fabric fabric = FabricFactory.create(0)) {
    NodeId worker = fabric.connectLocal();

    // Runnable — ignores return value
    Future<Void> f = fabric.executeAsync(worker, () -> {
        System.out.println("running on worker thread");
    });
    f.get(); // wait for completion

    // Callable — returns a value
    Future<Integer> result = fabric.executeAsync(worker, () -> 6 * 7);
    System.out.println(result.get()); // 42
}
```

Multiple local workers run concurrently:

```java
NodeId w1 = fabric.connectLocal();
NodeId w2 = fabric.connectLocal();

Future<Void> f1 = fabric.executeAsync(w1, () -> heavyTask("A"));
Future<Void> f2 = fabric.executeAsync(w2, () -> heavyTask("B"));
f1.get(); f2.get();
```

---

## Remote workers

Remote workers are JVMs running the `baton-agent` fat JAR. Obtain their `NodeId` via `fabric.deployAndConnect()` (SSH) or by having the agent register itself.

```java
// NodeId obtained from deployAndConnect or the agent registry
NodeId remote = ...;

// The lambda is serialised along with its captured class bytes
// and shipped to the remote agent over HTTP
Future<String> f = fabric.executeAsync(remote, () -> {
    return InetAddress.getLocalHost().getHostName(); // executes on the remote host
});
System.out.println(f.get()); // prints the remote host name
```

**Captured variables** must be `Serializable`. Distributed primitives obtained from `fabric.counter(...)` etc. in HTTP mode are already serialisable HTTP proxies and can be safely captured:

```java
DistributedCounter counter = fabric.counter("hits", 0L);

Future<Void> f = fabric.executeAsync(remote, () -> {
    // counter is an HttpCounterProxy — serialisable, calls back to orchestrator
    counter.incrementAndGet();
});
f.get();
System.out.println(counter.get()); // 1
```

---

## Exception handling

Any exception thrown inside the lambda is caught by the agent, serialised, and delivered as the cause of an `ExecutionException`:

```java
Future<Void> f = fabric.executeAsync(worker, () -> {
    throw new IllegalStateException("something went wrong");
});

try {
    f.get();
} catch (ExecutionException e) {
    Throwable cause = e.getCause(); // IllegalStateException("something went wrong")
    System.out.println(cause.getMessage());
}
```

---

## Dead agent detection

The orchestrator monitors agent heartbeats. If heartbeat grace is exceeded, the agent is declared dead and **all pending futures for that agent are failed**.

The current failure cause type is `AgentDeadException` inside `baton-core`. Treat it as an execution failure and inspect the cause:

```java
try {
    Future<Void> longRunning = fabric.executeAsync(remoteNode, () -> Thread.sleep(60_000));
    longRunning.get();
} catch (ExecutionException e) {
    Throwable cause = e.getCause();
    if (cause != null && "AgentDeadException".equals(cause.getClass().getSimpleName())) {
        System.out.println("Agent died: " + cause.getMessage());
    }
}
```

---

## Coordinating multiple jobs with a barrier

```java
int workers = 3;
NodeId[] nodes = { fabric.connectLocal(), fabric.connectLocal(), fabric.connectLocal() };

DistributedBarrier gate    = fabric.barrier("round", workers);
DistributedCounter counter = fabric.counter("count", 0L);

List<Future<Void>> futures = new ArrayList<>();
for (NodeId node : nodes) {
    futures.add(fabric.executeAsync(node, () -> {
        // All three jobs reach this point before any continues
        gate.await(10, TimeUnit.SECONDS);
        counter.incrementAndGet();
    }));
}

for (Future<Void> f : futures) f.get();
System.out.println(counter.get()); // 3
```
