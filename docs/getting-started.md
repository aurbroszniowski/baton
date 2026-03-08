# Getting Started

## Prerequisites

- Java 11+
- Gradle 8+ or Maven 3.6+

## Build and install locally

```bash
git clone https://github.com/aurbroszniowski/baton
cd baton
./gradlew publishToMavenLocal -x test
```

This installs all four modules to `~/.m2/repository/io/baton/`.

## Add the dependency

Only `baton-core` is needed — it declares `baton-api` as a transitive dependency.

**Gradle:**
```groovy
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation 'io.baton:baton-core:1.0.0-SNAPSHOT'
}
```

**Maven:**
```xml
<repositories>
  <repository>
    <id>local</id>
    <url>file://${user.home}/.m2/repository</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.baton</groupId>
  <artifactId>baton-core</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Your first Fabric

`FabricFactory.create(port)` starts the orchestrator's HTTP server on the given port (use `0` to let the OS pick a free port, or `-1` for a pure in-process fabric with no HTTP server).

```java
import io.baton.Fabric;
import io.baton.FabricFactory;
import io.baton.NodeId;

public class Hello {
    public static void main(String[] args) throws Exception {
        try (Fabric fabric = FabricFactory.create(0)) {

            // Register a local worker in the same JVM
            NodeId worker = fabric.connectLocal();

            // Execute a lambda on that worker
            fabric.executeAsync(worker, () ->
                System.out.println("Hello from Baton worker!")
            ).get();
        }
    }
}
```

## Running the tests

```bash
./gradlew test
```

The test suite covers:
- In-process primitives (`baton-core`)
- HTTP transport between two sides of the same JVM (`HttpTransportIT`)
- Two-process lambda shipping, file transfer, and close-time cleanup (`TwoProcessIT` in `baton-agent`)
- Barrier timeout detail propagation (`HttpTransportIT.httpBarrier_timeoutMessage_includesArrivedCount`)

Integration tests spawn extra JVMs and bind random local ports. If a run hangs/fails intermittently, retry with:

```bash
./gradlew test --rerun-tasks
```
