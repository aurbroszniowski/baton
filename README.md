# Baton — A Custom Coordinator-Centric Grid Framework

## Standalone-First Design

Baton can be used in any Java project that needs to execute lambdas on remote JVMs and coordinate them with distributed primitives, in a lean way.

---

## Current problematic

Remote code execution from a JVM is usually achieved through a few well-known patterns. The most common is RPC-style invocation, where a JVM calls a remote service using technologies such as REST, gRPC, messaging systems, or historically Java RMI; the remote system executes code that is already deployed there. Another approach is the worker or job model, where tasks or JARs are submitted to remote JVM workers (for example in build agents, distributed job systems, or compute frameworks like Spark). Less commonly, systems rely on remote agents that receive commands and execute them locally, or on dynamic class loading or plugin mechanisms that allow remote bytecode to be fetched and executed. To coordinate multiple JVM instances running such tasks, distributed coordination systems are typically used, such as ZooKeeper, etcd, or Consul, or distributed data grids like Hazelcast or Infinispan. These provide primitives like barriers, leader election, distributed locks, and atomic counters, which allow several JVMs to synchronize their work across a cluster.

However, these approaches introduce significant complexity. Distributed coordination primitives require network round trips and depend on consensus or strong consistency mechanisms, which adds latency and operational overhead. They also introduce difficult failure modes—timeouts, partial failures, split-brain situations, and retries—which make seemingly simple constructs like barriers or counters harder to reason about in a distributed environment. Running and maintaining coordination infrastructure, handling serialization compatibility, and ensuring proper security and observability further increase the cost of such systems.

For these reasons, a lean implementation can often be preferable. Reducing the number of coordination primitives and relying instead on simpler patterns—such as message-driven work distribution, idempotent tasks, and minimal shared state—lowers operational burden and reduces failure scenarios. Lean designs tend to be easier to operate, scale more predictably, and remain easier to understand and evolve over time. By minimizing global coordination and focusing on simple communication patterns, systems can achieve higher robustness while avoiding much of the complexity inherent to traditional distributed coordination frameworks.

---

## Design Philosophy

Baton uses a lean implementation to tackle this problematic

Three clean design principles follow:

1. **Coordinator-centric state** — All distributed primitives (counters, booleans, references, barriers, queues) live in the orchestrator's JVM. Agents access them via HTTP. No distributed consensus needed.
2. **Explicit class shipping** — Instead of peer class loading (which needs a cluster), the lambda's class bytes are serialized alongside the lambda instance. The agent deserializes using a temporary classloader over those bytes. Same effect, zero cluster infrastructure.
3. **HTTP everywhere** — The JDK ships with `com.sun.net.httpserver.HttpServer`. That is the entire transport layer.

**Single allowed external dependency:** `sshj` (`com.hierynomus:sshj`) — a pure-Java SSH/SCP library from a small independent maintainer. Used only for remote agent deployment. Optional: if Angela already provides SSH helpers, use those in the adapter instead.

---

## Repository Layout

```
baton/                          ← standalone project, published to Maven Central
├── baton-api/                  ← public API — pure Java, zero deps
├── baton-core/                 ← implements baton-api (JDK HttpServer)
├── baton-agent/                ← fat JAR deployed to remote hosts
└── baton-deployer/             ← SSH/SCP agent lifecycle (optional, uses sshj)
```

