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
 * JVM-wide Baton runtime state, accessible from any module that depends on
 * {@code baton-api}.
 *
 * <p>Provides a single static hook for emitting log lines to whichever
 * {@link AgentLogRelay} the running agent has installed:
 * <ul>
 *   <li>The fat-JAR path ({@code baton-agent}) installs the HTTP-posting
 *       {@code LogRelayClient} via {@code BatonAgentRuntime.install()}.
 *   <li>The in-process path ({@code BatonAgentFabric} in {@code baton-core})
 *       installs an inline relay that prints structured lines to stdout.
 * </ul>
 *
 * <p>Before any relay is installed all calls to {@link #emit} are no-ops.
 * This makes {@link #emit} safe to call from adapter code that may run in
 * non-Baton environments.
 */
public final class BatonRuntime {

    private static volatile AgentLogRelay relay = AgentLogRelay.NOOP;

    private BatonRuntime() {}

    /**
     * Install the shared relay.  Called once per agent JVM at startup — either
     * by {@code BatonAgentRuntime.install()} (fat-JAR path) or by
     * {@code BatonAgentFabric} (in-process path).
     *
     * <p>Replaces any previously installed relay, so the last caller wins.
     * In practice each JVM hosts exactly one agent and this is called once.
     */
    public static void install(AgentLogRelay logRelay) {
        relay = logRelay != null ? logRelay : AgentLogRelay.NOOP;
    }

    /**
     * Emit a log line through the installed relay, tagged with the current
     * {@link RemoteExecutionContext}.  No-op if no relay has been installed.
     *
     * @param source  producer type: {@code "framework"}, {@code "system-out"}, etc.
     * @param label   logger or subsystem name; {@code "-"} if not applicable
     * @param stream  {@code "stdout"}, {@code "stderr"}, or {@code "-"}
     * @param line    the log line text
     */
    public static void emit(String source, String label, String stream, String line) {
        relay.submit(source, label, stream, line);
    }
}
