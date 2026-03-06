/*
 * Copyright Aurelien Broszniowski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.baton;

import java.util.ServiceLoader;

/**
 * Entry point for creating a {@link Fabric}.
 *
 * <p>The implementation is loaded via {@link ServiceLoader} from the
 * {@code baton-core} module.  Add {@code baton-core} as a runtime
 * dependency to make {@link #create()} work.
 *
 * <pre>{@code
 * try (Fabric fabric = FabricFactory.create()) {
 *     NodeId worker = fabric.deployAndConnect("host", SshConfig.of("user", "~/.ssh/id_rsa"));
 *     fabric.executeAsync(worker, () -> System.out.println("Hello from remote!")).get();
 * }
 * }</pre>
 */
public final class FabricFactory {

    private FabricFactory() {}

    /** Creates a {@link Fabric} whose orchestrator binds to a randomly selected free port. */
    public static Fabric create() {
        return create(-1); // -1 = local-only, no HTTP server
    }

    /**
     * Creates a {@link Fabric} whose orchestrator listens on {@code orchestratorPort}.
     * Pass {@code 0} to pick any free port.
     */
    public static Fabric create(int orchestratorPort) {
        return ServiceLoader.load(FabricProvider.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No FabricProvider found on classpath. " +
                        "Add baton-core as a runtime dependency."))
                .create(orchestratorPort);
    }
}
