# Distributed Primitives

All primitives are obtained through `Fabric` and share state across all nodes connected to the same orchestrator. In HTTP mode the state lives in the orchestrator's in-memory store and is accessed via HTTP proxies; in local mode (port `-1`) everything is in-process.

---

## DistributedCounter

A 64-bit integer counter with atomic update operations.

```java
DistributedCounter c = fabric.counter("my-counter", 0L);

long v = c.get();                       // read current value
long n = c.incrementAndGet();           // add 1, return new value
long p = c.getAndIncrement();           // return old value, then add 1
long x = c.getAndSet(99L);             // swap, return old value
boolean ok = c.compareAndSet(99L, 0L); // CAS — returns true if swapped
```

Same name → same counter:
```java
DistributedCounter a = fabric.counter("shared", 0L);
DistributedCounter b = fabric.counter("shared", 0L); // same underlying counter
a.incrementAndGet();
assert b.get() == 1;
```

---

## DistributedBoolean

```java
DistributedBoolean flag = fabric.bool("flag", false);

boolean v   = flag.get();
flag.set(true);
boolean old = flag.getAndSet(false);           // returns previous value
boolean ok  = flag.compareAndSet(false, true); // CAS
```

---

## DistributedReference

Holds any `Serializable` value.

```java
DistributedReference<String> ref = fabric.reference("config", "default");

String v = ref.get();
ref.set("updated");
boolean ok = ref.compareAndSet("updated", "final"); // CAS

// Works with any Serializable type
DistributedReference<Integer> count = fabric.reference("count", 0);
count.set(42);
```

---

## DistributedBarrier

Synchronises exactly `parties` threads (or jobs) before any of them proceeds. Works across threads in the same JVM or across remote agents — each party calls `await()` and blocks until all have arrived.

```java
int parties = 3;
DistributedBarrier gate = fabric.barrier("start-gate", parties);

// In each of the 3 threads / jobs:
int index = gate.await();           // blocks until all 3 arrive; returns arrival index (0-based)

// With timeout:
int index = gate.await(5, TimeUnit.SECONDS); // throws TimeoutException if not all arrive in time
```

The `TimeoutException` message contains how many parties arrived: `"Barrier timeout: 1/3 arrived"`.

Barriers are **reusable** across generations — each time all parties arrive the generation advances and the barrier resets automatically.

---

## DistributedQueue

A FIFO blocking queue. Items can be produced and consumed from different threads or different JVMs.

```java
DistributedQueue<String> q = fabric.queue("work-queue");

// Producer
q.put("task-1");
q.put("task-2");

// Consumer (blocks until an item is available)
String item = q.take();

// Non-blocking poll with timeout
String item = q.poll(500, TimeUnit.MILLISECONDS); // null if nothing arrives in time
```

Items must be `Serializable` when sent across the HTTP boundary.
