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
package io.baton.agent;

import io.baton.BatonRuntime;

/**
 * Fat-JAR agent extension of {@link BatonRuntime}.
 *
 * <p>Adds {@link #attachProcess} — the ability to wire a child-process
 * stdout/stderr into the log relay — which requires {@link LogRelayClient}
 * and therefore cannot live in {@code baton-api}.
 *
 * <p>{@link #install} wires the relay into both this class (for
 * {@link #attachProcess}) and the shared {@link BatonRuntime} (for
 * {@link BatonRuntime#emit}, accessible from any module).
 *
 * <p>Typical framework usage (e.g. an Angela adapter):
 * <pre>{@code
 * Process server = new ProcessBuilder(...).start();
 * ProcessLogRelay relay = BatonAgentRuntime.attachProcess(server, "tc-server");
 * // ... wait for server ...
 * if (relay != null) relay.awaitEof(30, TimeUnit.SECONDS);
 * }</pre>
 *
 * <p>All methods are no-ops before {@link AgentMain} calls {@link #install},
 * making this safe to call from adapter code that may run in non-Baton
 * environments.
 */
public final class BatonAgentRuntime {

    /** Kept for {@link #attachProcess}, which needs the concrete type. */
    private static volatile LogRelayClient logRelayClient;

    private BatonAgentRuntime() {}

    // ── Lifecycle (package-private, called by AgentMain) ──────────────────────

    /**
     * Install the shared relay.  Called once by {@link AgentMain} at startup.
     * Also installs the relay into {@link BatonRuntime} so that
     * {@link BatonRuntime#emit} is available from any module.
     */
    static void install(LogRelayClient relay) {
        logRelayClient = relay;
        BatonRuntime.install(relay); // LogRelayClient implements AgentLogRelay
    }

    // ── Public API for framework / adapter code ───────────────────────────────

    /**
     * Attach a child process to the shared log relay using the current
     * {@link io.baton.RemoteExecutionContext} as the job identifier.
     *
     * <p>Returns {@code null} if no relay is installed (agent not started, or
     * called from a non-Baton environment).
     *
     * @param process      the running child process
     * @param processLabel short label used in log output, e.g. {@code "tc-server"}
     * @return a started {@link ProcessLogRelay}, or {@code null} if unavailable
     */
    public static ProcessLogRelay attachProcess(Process process, String processLabel) {
        LogRelayClient r = logRelayClient;
        if (r == null) return null;
        return ProcessLogRelay.attach(process, r, processLabel);
    }

    /**
     * Emit a single log line.  Delegates to {@link BatonRuntime#emit}.
     * No-op if no relay is installed.
     *
     * @param source  producer type: {@code "framework"}, {@code "system-out"}, etc.
     * @param label   logger or subsystem name; {@code "-"} if not applicable
     * @param stream  {@code "stdout"}, {@code "stderr"}, or {@code "-"}
     * @param line    the log line text
     */
    public static void emit(String source, String label, String stream, String line) {
        BatonRuntime.emit(source, label, stream, line);
    }
}
