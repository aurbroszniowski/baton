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
package io.baton.core;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LogCollector}, {@link PrintingLogSink}, and
 * the {@code /agent/logs} HTTP endpoint on {@link BatonFabric}.
 */
class LogCollectorTest {

    // ── PrintingLogSink ───────────────────────────────────────────────────────

    @Test
    void printingLogSink_formatsLine() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8));

        sink.accept(new LogRecord("baton-agent-8700@host#123", "5f193fdc",
                "process", "tc-server", "stderr", "Moved to ACTIVE"));

        String output = buf.toString(StandardCharsets.UTF_8);
        assertEquals("[baton-agent-8700@host#123][5f193fdc][tc-server] Moved to ACTIVE",
                output.trim());
    }

    @Test
    void printingLogSink_usesDashForMissingFields() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8));

        sink.accept(new LogRecord(null, null, "system-out", null, null, "hello"));

        String output = buf.toString(StandardCharsets.UTF_8).trim();
        assertEquals("[-][-][-] hello", output);
    }

    // ── PrintingLogSink jobId abbreviation ────────────────────────────────────

    @Test
    void printingLogSink_abbreviatesUuidForDisplay() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8));

        // Full UUID in the record — display must show only first 8 chars
        sink.accept(new LogRecord("node", "73243b5e-2a8e-453c-bc08-ded0bda5d0ed",
                "system-out", "-", "-", "a line"));

        String out = buf.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("[73243b5e]"), "UUID must be abbreviated to 8 chars in display");
        assertFalse(out.contains("2a8e"), "Full UUID must not appear verbatim in display output");
    }

    @Test
    void printingLogSink_shortJobId_keptAsIs() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8));

        sink.accept(new LogRecord("node", "my-job", "system-out", "-", "-", "line"));

        String out = buf.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("[my-job]"), "Short job IDs must not be truncated");
    }

    @Test
    void abbreviate_uuidTruncatesTo8() {
        assertEquals("73243b5e", PrintingLogSink.abbreviate("73243b5e-2a8e-453c-bc08-ded0bda5d0ed"));
    }

    @Test
    void abbreviate_shortIdUnchanged() {
        assertEquals("abc", PrintingLogSink.abbreviate("abc"));
        assertEquals("-",   PrintingLogSink.abbreviate("-"));
    }

    // ── PrintingLogSink ANSI ──────────────────────────────────────────────────

    @Test
    void printingLogSink_ansi_stderrIsRed() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8))
                .withAnsi();

        sink.accept(new LogRecord("node", "job1", "system-err", "-", "-", "error line"));

        String out = buf.toString(StandardCharsets.UTF_8);
        assertTrue(out.startsWith("\u001B[31m"), "stderr line should start with red ANSI code");
        assertTrue(out.contains("\u001B[0m"),    "line should end with ANSI reset");
        assertTrue(out.contains("error line"));
    }

    @Test
    void printingLogSink_ansi_frameworkIsCyan() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8))
                .withAnsi();

        sink.accept(new LogRecord("node", "job1", "framework", "com.example.Foo", "-", "log msg"));

        String out = buf.toString(StandardCharsets.UTF_8);
        assertTrue(out.startsWith("\u001B[36m"), "framework line should start with cyan ANSI code");
    }

    @Test
    void printingLogSink_ansi_stdoutIsWhite() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8))
                .withAnsi();

        sink.accept(new LogRecord("node", "job1", "system-out", "-", "-", "normal line"));

        String out = buf.toString(StandardCharsets.UTF_8);
        assertTrue(out.startsWith("\u001B[37m"), "stdout line should start with white ANSI code");
    }

    @Test
    void printingLogSink_noAnsi_noEscapeCodes() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintingLogSink sink = new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8));

        sink.accept(new LogRecord("node", "job1", "system-err", "-", "-", "error line"));

        String out = buf.toString(StandardCharsets.UTF_8);
        assertFalse(out.contains("\u001B"), "plain sink must not contain ANSI escape codes");
    }

    @Test
    void batonFabric_enableAnsiColour_appliesColour() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        BatonFabric fabric = new BatonFabric(0);
        // Replace default System.out sink with one backed by our buffer
        fabric.setLogSink(new PrintingLogSink(new PrintStream(buf, true, StandardCharsets.UTF_8)));
        fabric.enableAnsiColour();

        try {
            HttpURLConnection conn =
                    (HttpURLConnection) new URL(fabric.getOrchestratorUrl() + "/agent/logs").openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type",  "text/plain; charset=utf-8");
            conn.setRequestProperty("X-Node-Id", "n");
            conn.setRequestProperty("X-Job-Id",  "-");
            conn.setRequestProperty("X-Source",  "system-err");
            conn.setRequestProperty("X-Label",   "-");
            conn.setRequestProperty("X-Stream",  "-");
            byte[] body = "crash\n".getBytes(StandardCharsets.UTF_8);
            conn.setRequestProperty("Content-Length", Integer.toString(body.length));
            try (OutputStream out = conn.getOutputStream()) { out.write(body); }
            conn.getResponseCode();
        } finally {
            fabric.close();
        }

        Thread.sleep(200); // let the sink process
        String out = buf.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("\u001B[31m"), "system-err output must be red after enableAnsiColour()");
    }

    // ── LogCollector ──────────────────────────────────────────────────────────

    @Test
    void logCollector_dispatchesToSink() {
        List<LogRecord> received = new ArrayList<>();
        LogCollector collector = new LogCollector(received::add);

        LogRecord record = new LogRecord("node1", "job1", "process", "server", "stdout", "line1");
        collector.collect(record);

        assertEquals(1, received.size());
        assertSame(record, received.get(0));
    }

    @Test
    void logCollector_swapSink() {
        List<LogRecord> first  = new ArrayList<>();
        List<LogRecord> second = new ArrayList<>();
        LogCollector collector = new LogCollector(first::add);

        collector.collect(new LogRecord("n", "j", "s", "l", "st", "a"));
        collector.setSink(second::add);
        collector.collect(new LogRecord("n", "j", "s", "l", "st", "b"));

        assertEquals(1, first.size());
        assertEquals(1, second.size());
        assertEquals("a", first.get(0).line);
        assertEquals("b", second.get(0).line);
    }

    @Test
    void logCollector_sinkExceptionIsSwallowed() {
        LogCollector collector = new LogCollector(r -> { throw new RuntimeException("boom"); });
        // Must not throw
        assertDoesNotThrow(() ->
                collector.collect(new LogRecord("n", "j", "s", "l", "st", "line")));
    }

    @Test
    void logCollector_nullSinkIsNoop() {
        LogCollector collector = new LogCollector(null);
        assertDoesNotThrow(() ->
                collector.collect(new LogRecord("n", "j", "s", "l", "st", "line")));
    }

    // ── BatonFabric /agent/logs endpoint ─────────────────────────────────────

    @Test
    void agentLogs_endpointAcceptsBatch() throws Exception {
        List<LogRecord> received = new ArrayList<>();
        BatonFabric fabric = new BatonFabric(0);
        fabric.setLogSink(received::add);
        String baseUrl = fabric.getOrchestratorUrl();

        try {
            // POST three lines to /agent/logs
            HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + "/agent/logs").openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            conn.setRequestProperty("X-Node-Id", "agent-1@host:9000#42");
            conn.setRequestProperty("X-Job-Id",  "abcd1234");
            conn.setRequestProperty("X-Source",  "process");
            conn.setRequestProperty("X-Label",   "tc-server");
            conn.setRequestProperty("X-Stream",  "stderr");
            byte[] body = "line one\nline two\nline three\n".getBytes(StandardCharsets.UTF_8);
            conn.setRequestProperty("Content-Length", Integer.toString(body.length));
            try (OutputStream out = conn.getOutputStream()) {
                out.write(body);
            }
            assertEquals(200, conn.getResponseCode());
        } finally {
            fabric.close();
        }

        assertEquals(3, received.size(), "Expected 3 records, got: " + received.size());
        LogRecord r0 = received.get(0);
        assertEquals("agent-1@host:9000#42", r0.nodeId);
        assertEquals("abcd1234",             r0.jobId);
        assertEquals("process",              r0.source);
        assertEquals("tc-server",            r0.label);
        assertEquals("stderr",               r0.stream);
        assertEquals("line one",             r0.line);
        assertEquals("line two",             received.get(1).line);
        assertEquals("line three",           received.get(2).line);
    }

    @Test
    void agentLogs_emptyLinesAreSkipped() throws Exception {
        List<LogRecord> received = new ArrayList<>();
        BatonFabric fabric = new BatonFabric(0);
        fabric.setLogSink(received::add);
        String baseUrl = fabric.getOrchestratorUrl();

        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + "/agent/logs").openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
            conn.setRequestProperty("X-Node-Id", "node");
            conn.setRequestProperty("X-Job-Id",  "-");
            conn.setRequestProperty("X-Source",  "system-out");
            conn.setRequestProperty("X-Label",   "-");
            conn.setRequestProperty("X-Stream",  "-");
            byte[] body = "\nhello\n\nworld\n".getBytes(StandardCharsets.UTF_8);
            conn.setRequestProperty("Content-Length", Integer.toString(body.length));
            try (OutputStream out = conn.getOutputStream()) {
                out.write(body);
            }
            conn.getResponseCode();
        } finally {
            fabric.close();
        }

        assertEquals(2, received.size());
        assertEquals("hello", received.get(0).line);
        assertEquals("world", received.get(1).line);
    }
}
