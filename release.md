# Releasing Baton to Maven Central

Baton publishes to **Maven Central** via the **Sonatype Central Portal**, using
Gradle's `maven-publish` plugin together with
[`io.github.gradle-nexus.publish-plugin`](https://github.com/gradle-nexus/publish-plugin).
Releases are driven by **GitHub Actions** on tag push.

The build, signing, POM metadata, and publishing repositories are already wired
up in `build.gradle` and `.github/workflows/release.yml`. This document is the
release runbook: what to do when cutting a new version.

Useful Sonatype references:

- Central Portal publishing: https://central.sonatype.org/publish/publish-portal-gradle/
- OSSRH staging API compatibility: https://central.sonatype.org/publish/publish-portal-ossrh-staging-api/
- Central publishing requirements: https://central.sonatype.org/publish/requirements/
- Immutability of published releases: https://central.sonatype.org/publish/requirements/immutability/

## What gets published

| Module        | Published? | Notes                                                                 |
|---------------|------------|-----------------------------------------------------------------------|
| `baton-api`   | Yes        | Pure interfaces, zero deps. Pulled in transitively via `baton-core`.  |
| `baton-core`  | Yes        | Orchestrator runtime. The only artifact consumers need to declare.    |
| `baton-agent` | No         | Internal fat JAR, embedded as a classpath resource inside `baton-core`. |

`baton-agent` is excluded by skipping the `maven-publish` / `signing` plugins
for that subproject (see `build.gradle`).

## Prerequisites (one-time, per maintainer / repository)

These are configured **once** — verify they are still in place before your first
release, but you do not need to redo them every time.

1. **Sonatype namespace.** The `io.github.<owner>.baton` namespace must be
   verified in https://central.sonatype.com under `Publishing -> Namespaces`.
2. **Portal user token.** Generated at https://central.sonatype.com/usertoken.
   Store the username/password the modal shows you — they cannot be retrieved
   afterwards.
3. **GPG signing key.** An ASCII-armored private key plus its passphrase. The
   public key must be published to a keyserver Sonatype can reach (e.g.
   `keys.openpgp.org`).
4. **GitHub repository secrets.** In `Settings -> Secrets and variables -> Actions`:
   - `CENTRAL_USERNAME` — Portal token username
   - `CENTRAL_PASSWORD` — Portal token password
   - `SIGNING_KEY` — ASCII-armored private key contents
   - `SIGNING_PASSWORD` — passphrase for the signing key

The release workflow exposes these secrets as `ORG_GRADLE_PROJECT_*`
environment variables so Gradle picks them up as project properties without
ever writing them to disk.

## Release flow (per release)

The version lives in `gradle.properties`. Releases are normal commits whose
version is non-`SNAPSHOT`, tagged `vX.Y.Z`, with a follow-up commit that bumps
back to the next snapshot.

### 1. Decide versions

```bash
export RELEASE_VERSION=1.0.3
export NEXT_VERSION=1.0.4-SNAPSHOT
```

### 2. Local preflight

From a clean worktree:

```bash
./gradlew clean build
./gradlew publishToMavenLocal
```

Confirm each published module produced a main jar, sources jar, and javadoc jar
under `<module>/build/libs/`, and that the POM in `~/.m2/repository/...`
contains `name`, `description`, `url`, `licenses`, `developers`, and `scm`.

### 3. Set the release version and commit

Edit `gradle.properties`:

```properties
version=1.0.3
```

```bash
git diff gradle.properties
git commit -am "Release ${RELEASE_VERSION}"
```

### 4. Tag and push

The release workflow triggers on tags matching `v*`.

```bash
git tag "v${RELEASE_VERSION}"
git push origin HEAD
git push origin "v${RELEASE_VERSION}"
```

### 5. Watch the workflow

Open the **Actions** tab on GitHub and follow the *Release to Maven Central*
run. It executes:

```bash
./gradlew --no-daemon clean build
./gradlew --no-daemon publishToSonatype closeAndReleaseSonatypeStagingRepository
```

`closeAndReleaseSonatypeStagingRepository` closes the staging repository,
validates the deployment, and releases it to Maven Central. Verify in
https://central.sonatype.com/publishing/deployments. Propagation to
`https://repo.maven.apache.org/maven2/` typically takes a few minutes; index
visibility on `search.maven.org` may take longer.

> **Central releases are immutable.** Do not retry the same version after it
> reports as published. If something is wrong, cut a new patch version.

### 6. Bump back to snapshot

```bash
# edit gradle.properties: version=${NEXT_VERSION}
git commit -am "Bump to snapshot (${NEXT_VERSION})"
git push origin HEAD
```

## Recovering from a failed release (before it is published)

If the workflow fails *before* the staging repository is released, the version
has not yet been published to Central and you can reuse the same version
number. Fix the issue, then move the tag:

```bash
# Delete the broken tag locally and remotely
git tag -d "v${RELEASE_VERSION}"
git push origin ":refs/tags/v${RELEASE_VERSION}"

# Recreate on the fixed commit and push
git tag "v${RELEASE_VERSION}"
git push origin "v${RELEASE_VERSION}"
```

If the workflow already released the staging repository (the deployment shows
as published in the Central Portal), the version is permanent — bump to the
next patch instead.

## Snapshot publishing

Snapshots (`*-SNAPSHOT`) go to Central's snapshot repository
(`https://central.sonatype.com/repository/maven-snapshots/`) which is
configured in `build.gradle` under `nexusPublishing { sonatype { ... } }`. To
publish a snapshot manually with credentials supplied via Gradle properties:

```bash
./gradlew publishToSonatype \
  -PsonatypeUsername=... -PsonatypePassword=... \
  -PsigningKey="$(cat key.asc)" -PsigningPassword=...
```

In day-to-day work, prefer `./gradlew publishToMavenLocal` for local
integration testing.

## Local debug helpers

Split the staging close/release for inspection:

```bash
./gradlew publishToSonatype closeSonatypeStagingRepository
# inspect at https://central.sonatype.com/publishing/deployments
./gradlew findSonatypeStagingRepository releaseSonatypeStagingRepository
```

## Common pitfalls

- **Missing javadoc/sources jars.** Already enabled via `withSourcesJar()` /
  `withJavadocJar()` — but if you add a new published module, make sure it
  inherits the `subprojects { java { ... } }` block.
- **Missing POM metadata.** Central rejects bundles without `licenses`,
  `developers`, and `scm`. The `pom { ... }` block in `build.gradle` covers
  these for every published module.
- **Unsigned artifacts.** The signing plugin only activates when the build
  graph contains a publish task and `signingKey` / `signingPassword` are set;
  local `publishToMavenLocal` runs unsigned, but Sonatype publishing must be
  signed.
- **Wrong credentials.** Use the **Central Portal user token**, not legacy
  OSSRH credentials. The token's username/password go into `CENTRAL_USERNAME`
  / `CENTRAL_PASSWORD`.
- **Re-tagging after publication.** Central is immutable — bump the version
  instead of trying to re-release the same one.
