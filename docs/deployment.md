# SSH Deployment

SSH deployment is built into `baton-core`. The `AgentDeployer` and `SshAgentLauncher` classes ship the `baton-agent` fat JAR to remote hosts and start agents automatically, no extra dependency required.

---

## Dependency

Just declare `baton-core`:

**Gradle:**
```groovy
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

---

## SshConfig

`SshConfig` carries the credentials needed to open an SSH connection:

```java
// Default SSH port (22), private-key auth
SshConfig ssh = SshConfig.of("alice", "~/.ssh/id_rsa");

// Custom port
SshConfig ssh = SshConfig.of("alice", "~/.ssh/id_rsa", 2222);
```

---

## AgentDeployer

`AgentDeployer` handles three steps automatically:

1. SCPs the fat JAR to `~/baton/agent.jar` on the remote host
2. Starts the agent with `nohup java -jar agent.jar --orchestrator <url> --port <port>`
3. Polls `GET /health` and returns a reachable `NodeId` (30 s timeout)

```java
import io.baton.deployer.AgentDeployer;

AgentDeployer deployer = new AgentDeployer(Path.of("baton-agent-1.0.0-SNAPSHOT-fat.jar"));

NodeId remote = deployer.deploy(
    "server-a.example.com",
    SshConfig.of("alice", "~/.ssh/id_rsa"),
    "http://orchestrator.example.com:9400"   // your orchestrator URL
);

// remote is now reachable on its /health endpoint, run jobs on it
Future<String> f = fabric.executeAsync(remote, () ->
    InetAddress.getLocalHost().getHostName()
);
System.out.println(f.get()); // prints "server-a"

// Shut the agent down when done
deployer.stop(remote);
```

---

## fabric.deployAndConnect()

`Fabric` exposes a convenience wrapper that internally delegates to the `AgentLauncher` SPI (resolved via `ServiceLoader`). `SshAgentLauncher` is registered automatically.

The agent fat JAR is embedded inside `baton-core` and extracted to a temporary file automatically: no manual download or system property required:

```java
try (Fabric fabric = FabricFactory.create(9400)) {

    SshConfig ssh = SshConfig.of("alice", "~/.ssh/id_rsa");

    // Deploys the embedded agent JAR and returns once the agent is registered
    NodeId remote = fabric.deployAndConnect("server-a.example.com", ssh);

    Future<Integer> f = fabric.executeAsync(remote, () -> Runtime.getRuntime().availableProcessors());
    System.out.println("Remote CPUs: " + f.get());
}
```

To use a custom agent build instead, set the system property `baton.agent.jar` to override the embedded JAR:

```bash
java -Dbaton.agent.jar=/path/to/custom-agent.jar -jar your-app.jar
```

---

## File transfer

`Fabric` also provides `upload` and `download` helpers that stream files to/from the **agent HTTP endpoint** (`http://<agent-host>:<agent-port>/files`):

```java
// Upload a local file to the remote agent's working directory
fabric.upload(remote, Path.of("/local/data/dataset.csv"), "data/dataset.csv");

// Download a result file back
fabric.download(remote, "results/output.csv", Path.of("/local/results/output.csv"));
```

---

## Agent log

The agent process writes its output to `~/baton/agent.log` on the remote host. Check this file if an agent fails to register within the 30 s timeout.

---

## Operational caveats

- `AgentDeployer` currently starts agents on a fixed remote port (`8700`).
- SSH host key verification is currently permissive (`PromiscuousVerifier`); tighten this before production use.
