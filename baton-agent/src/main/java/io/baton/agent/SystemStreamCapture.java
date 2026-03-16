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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Installs tee wrappers around {@link System#out} and {@link System#err} so
 * that every line written to either stream is also relayed to the orchestrator
 * via {@link LogRelayClient}.
 *
 * <p>The originals are preserved for local output — this is a pure tee, not a
 * replacement.  Agent startup messages, stack traces, and any in-JVM
 * {@code System.out.println} inside a job are all captured.
 *
 * <p>Call {@link #install} once at agent startup, before the first job runs.
 * After installation the relay is the only thing that changes; local output
 * (to the agent's own terminal / log file) is unaffected.
 *
 * <p>Lines are accumulated per-thread until a {@code '\n'} is detected, then
 * submitted to the relay as a single record tagged with the current
 * {@link io.baton.RemoteExecutionContext}.
 */
public final class SystemStreamCapture {

    private SystemStreamCapture() {}

    /**
     * Replace {@link System#out} and {@link System#err} with tee streams.
     * Idempotent: calling again after the first install is a no-op (the
     * streams are already wrappers, not the raw originals).
     *
     * @param relay the agent's shared log relay
     */
    public static void install(LogRelayClient relay) {
        if (!(System.out instanceof TeePrintStream)) {
            System.setOut(new TeePrintStream(System.out, relay, "system-out"));
        }
        if (!(System.err instanceof TeePrintStream)) {
            System.setErr(new TeePrintStream(System.err, relay, "system-err"));
        }
    }

    // ── Internal tee stream ───────────────────────────────────────────────────

    static final class TeePrintStream extends PrintStream {

        private final LogRelayClient     relay;
        private final String             source;
        /** Per-instance line accumulator; access guarded by {@code this}. */
        private final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream(256);

        TeePrintStream(PrintStream delegate, LogRelayClient relay, String source) {
            // autoFlush=true keeps behaviour identical to the replaced stream.
            super(delegate, true, StandardCharsets.UTF_8);
            this.relay  = relay;
            this.source = source;
        }

        @Override
        public synchronized void write(int b) {
            super.write(b);
            if (b == '\n') {
                emitLine();
            } else if (b != '\r') {
                lineBuffer.write(b);
            }
        }

        @Override
        public synchronized void write(byte[] buf, int off, int len) {
            super.write(buf, off, len);
            for (int i = off; i < off + len; i++) {
                byte b = buf[i];
                if (b == '\n') {
                    emitLine();
                } else if (b != '\r') {
                    lineBuffer.write(b);
                }
            }
        }

        private void emitLine() {
            if (lineBuffer.size() == 0) return;
            String line = lineBuffer.toString(StandardCharsets.UTF_8);
            lineBuffer.reset();
            relay.submit(source, "-", "-", line);
        }
    }
}
