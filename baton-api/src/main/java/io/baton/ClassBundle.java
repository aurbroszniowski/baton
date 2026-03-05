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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Wire-protocol DTO: a serialized lambda together with the bytecode of every
 * non-JDK class it references.
 *
 * <p>The orchestrator builds a {@code ClassBundle} for each outgoing job and POSTs
 * it (binary-serialized) to the agent.  The agent reconstructs the classloader from
 * {@link #classes} and deserializes {@link #lambda} to obtain the runnable.
 *
 * <p>Lives in {@code baton-api} (zero external dependencies) so that both
 * {@code baton-core} (which builds bundles) and {@code baton-agent}
 * (which consumes them) can reference it without a circular dependency.
 */
public final class ClassBundle implements Serializable {

    private static final long serialVersionUID = 1L;

    /** className (binary, dots) → bytecode of each non-JDK class the lambda touches. */
    public final Map<String, byte[]> classes;

    /** Serialized lambda instance (produced by {@code ClassCollector} in baton-core). */
    public final byte[] lambda;

    public ClassBundle(Map<String, byte[]> classes, byte[] lambda) {
        this.classes = Collections.unmodifiableMap(classes);
        this.lambda  = lambda;
    }
}
