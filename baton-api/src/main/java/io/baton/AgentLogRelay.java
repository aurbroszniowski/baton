/*
 * Copyright Aurélien Broszniowski
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

/**
 * Minimal log-relay seam used by {@link BatonRuntime}.
 *
 * <p>Lives in {@code baton-api} so both the agent fat-JAR path
 * ({@code baton-agent}'s {@code LogRelayClient}) and the in-process fabric path
 * ({@code baton-core}'s {@code BatonAgentFabric}) can provide an implementation
 * without a cross-module dependency.
 *
 * <p>The 4-argument overload is context-aware: implementations are expected to
 * read the current {@link RemoteExecutionContext#jobId()} themselves rather than
 * receiving it as a parameter.
 */
@FunctionalInterface
public interface AgentLogRelay {

    /**
     * Submit a log line for relay, using the current
     * {@link RemoteExecutionContext} as the job identifier.
     *
     * @param source  producer type: {@code "process"}, {@code "system-out"},
     *                {@code "framework"}, etc.
     * @param label   process name or logger name; {@code "-"} if not applicable
     * @param stream  {@code "stdout"}, {@code "stderr"}, or {@code "-"}
     * @param line    the log line text
     */
    void submit(String source, String label, String stream, String line);

    /** No-op relay used as the default before any agent installs a real one. */
    AgentLogRelay NOOP = (source, label, stream, line) -> {};
}
