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

1. SCPs the fat JAR to `<workDir>/agent.jar` and application JARs to `<workDir>/lib/` (`workDir` defaults to `~/baton`)
2. Starts the agent with `nohup java -cp <workDir>/agent.jar:<application-jars> io.baton.agent.AgentMain --orchestrator <url> --port <port>`
3. Polls `GET /identity` and returns the actual registered `NodeId` (30 s timeout)

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

The normal Fabric path starts the process, then waits up to 30 seconds for its
registration in the orchestrator. It returns that registered identity or fails;
it never returns a placeholder PID. `AgentDeployer.start()` returns only an
`AgentEndpoint` for code that needs to manage startup separately.

Remote initializers finish before the agent registers or exposes a ready
`/identity`. The first job dispatched after registration can therefore use the
framework state installed by those initializers. Initializer exceptions retain
their existing best-effort behavior: they are logged and remaining initializers
still run. The direct `AgentDeployer.deploy()` compatibility API continues to
poll `/identity`; registration waiting belongs to the orchestrator on the normal
Fabric path.

### Explicit application inputs

Framework adapters can supply the remote classpath, JVM properties, and working
directory instead of relying on discovery from the orchestrator process:

```java
DeploymentOptions options = new DeploymentOptions(
    List.of(Path.of("application.jar"), Path.of("logging-adapter.jar")),
    Map.of("application.mode", "test", "baton.path.data", "${workDir}/data"),
    "baton/application-run"
);
NodeId remote = fabric.deployAndConnect("server-a.example.com", ssh, options);
```

Relative working directories are resolved under the remote user's home. Property
values may contain `${workDir}`, expanded to the absolute remote directory.
The agent process and remote initializers use that directory. Aliases configured
with `baton.path.<name>` resolve paths such as `data://input.csv` in file transfers.
Alias suffixes must be relative and stay within their configured root after
normalization. Absolute suffixes and `..` escapes are rejected. This check is
lexical; aliases do not form a filesystem sandbox or prevent symlink traversal.

The original two-argument `deployAndConnect()` retains classpath discovery and
Angela property propagation for compatibility. Existing `angela-root://` file
paths also remain supported. Explicit options use only the supplied application
JARs and properties. The agent's API, shared runtime, and transport are already
included in its fat JAR.

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

The agent process writes its output to `<workDir>/agent.log` on the remote host
(`~/baton/agent.log` with default options). Check this file if an agent fails to
register within the 30 s timeout.

---

## Operational caveats

- `AgentDeployer` currently starts agents on a fixed remote port (`8700`).
- SSH host key verification is currently permissive (`PromiscuousVerifier`); tighten this before production use.
- Agent HTTP endpoints assume trusted peers. `/files` remains unauthenticated and
  accepts ordinary absolute paths; restricting alias suffixes does not add access control.
