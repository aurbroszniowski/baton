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

import io.baton.agent.SystemStreamCapture.TeePrintStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SystemStreamCapture}.
 */
class SystemStreamCaptureTest {

    private PrintStream originalOut;
    private PrintStream originalErr;

    @BeforeEach
    void saveStreams() {
        originalOut = System.out;
        originalErr = System.err;
    }

    @AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    /** Creates a stub relay that collects submitted lines. */
    private static CollectingRelay relay() {
        return new CollectingRelay();
    }

    static class CollectingRelay extends LogRelayClient {
        final List<String[]> records = new CopyOnWriteArrayList<>();

        CollectingRelay() {
            // null orchestrator URL — we never actually start this relay
            super("http://localhost:0", "test-agent@localhost:0#0");
        }

        @Override
        public void submit(String source, String label, String stream, String line) {
            records.add(new String[]{source, label, stream, line});
        }

        @Override
        public void submit(String jobId, String source, String label, String stream, String line) {
            records.add(new String[]{source, label, stream, line});
        }
    }

    // ── install() ─────────────────────────────────────────────────────────────

    @Test
    void install_replacesSystemOut() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);
        assertInstanceOf(TeePrintStream.class, System.out,
                "System.out should be a TeePrintStream after install");
    }

    @Test
    void install_replacesSystemErr() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);
        assertInstanceOf(TeePrintStream.class, System.err,
                "System.err should be a TeePrintStream after install");
    }

    @Test
    void install_isIdempotent() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);
        PrintStream firstOut = System.out;
        SystemStreamCapture.install(r);
        assertSame(firstOut, System.out, "Second install() should not replace the stream again");
    }

    // ── TeePrintStream line relay ──────────────────────────────────────────────

    @Test
    void println_relaysLine() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);

        System.out.println("hello-relay");

        assertFalse(r.records.isEmpty(), "Expected at least one relayed record");
        boolean found = r.records.stream().anyMatch(rec -> "hello-relay".equals(rec[3]));
        assertTrue(found, "Expected 'hello-relay' in " + r.records);
    }

    @Test
    void localOutputStream_stillReceivesOutput() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream localOut = new PrintStream(buf, true, StandardCharsets.UTF_8);
        System.setOut(localOut);

        CollectingRelay r = relay();
        SystemStreamCapture.install(r);

        System.out.println("local-check");

        assertTrue(buf.toString(StandardCharsets.UTF_8).contains("local-check"),
                "Local stream must still receive the output (tee, not replace)");
    }

    @Test
    void multipleLines_eachRelayedSeparately() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);

        System.out.println("line-one");
        System.out.println("line-two");
        System.out.println("line-three");

        long count = r.records.stream()
                .filter(rec -> rec[3].startsWith("line-"))
                .count();
        assertEquals(3, count, "Expected 3 relayed lines, got " + count);
    }

    @Test
    void emptyLinesAreNotRelayed() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);

        // Write a bare newline — should produce no relay record
        System.out.print("\n");

        long empty = r.records.stream().filter(rec -> rec[3].isEmpty()).count();
        assertEquals(0, empty, "Empty lines must not be relayed");
    }

    @Test
    void source_isSystemOut_forStdout() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);

        System.out.println("stdout-source-check");

        boolean found = r.records.stream()
                .anyMatch(rec -> "system-out".equals(rec[0]) && "stdout-source-check".equals(rec[3]));
        assertTrue(found, "Record should have source='system-out'");
    }

    @Test
    void source_isSystemErr_forStderr() {
        CollectingRelay r = relay();
        SystemStreamCapture.install(r);

        System.err.println("stderr-source-check");

        boolean found = r.records.stream()
                .anyMatch(rec -> "system-err".equals(rec[0]) && "stderr-source-check".equals(rec[3]));
        assertTrue(found, "Record should have source='system-err'");
    }
}
