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

import io.baton.AgentLogRelay;
import io.baton.BatonRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JulRelayHandler}.
 */
class JulRelayHandlerTest {

    private List<String[]> emitted;

    @BeforeEach
    void captureRelay() {
        emitted = new ArrayList<>();
        BatonRuntime.install((source, label, stream, line) ->
                emitted.add(new String[]{source, label, stream, line}));
    }

    @AfterEach
    void restoreRelay() {
        BatonRuntime.install(AgentLogRelay.NOOP);
    }

    private static LogRecord record(String loggerName, Level level, String msg) {
        LogRecord r = new LogRecord(level, msg);
        r.setLoggerName(loggerName);
        return r;
    }

    // ── basic publish ──────────────────────────────────────────────────────────

    @Test
    void publish_emitsMessageLine() {
        JulRelayHandler h = new JulRelayHandler();
        h.publish(record("com.example.Foo", Level.INFO, "simple message"));

        assertEquals(1, emitted.size());
        String[] rec = emitted.get(0);
        assertEquals("framework",       rec[0], "source");
        assertEquals("com.example.Foo", rec[1], "label (logger name)");
        assertEquals("-",               rec[2], "stream");
        assertEquals("simple message",  rec[3], "line");
    }

    @Test
    void publish_nullRecord_isIgnored() {
        JulRelayHandler h = new JulRelayHandler();
        assertDoesNotThrow(() -> h.publish(null));
        assertTrue(emitted.isEmpty());
    }

    @Test
    void publish_nullLoggerName_usesHyphen() {
        JulRelayHandler h = new JulRelayHandler();
        LogRecord r = new LogRecord(Level.INFO, "no logger");
        r.setLoggerName(null);
        h.publish(r);

        assertEquals(1, emitted.size());
        assertEquals("-", emitted.get(0)[1], "null logger name should become '-'");
    }

    // ── level filter ──────────────────────────────────────────────────────────

    @Test
    void publish_belowLevel_isFiltered() {
        JulRelayHandler h = new JulRelayHandler();
        h.setLevel(Level.WARNING);

        h.publish(record("x", Level.FINE, "filtered out"));

        assertTrue(emitted.isEmpty(), "Record below handler level must be filtered");
    }

    @Test
    void publish_atOrAboveLevel_isEmitted() {
        JulRelayHandler h = new JulRelayHandler();
        h.setLevel(Level.WARNING);

        h.publish(record("x", Level.WARNING, "not filtered"));
        h.publish(record("x", Level.SEVERE, "not filtered either"));

        assertEquals(2, emitted.size());
    }

    // ── multi-line splitting ───────────────────────────────────────────────────

    @Test
    void publish_multiLineMessage_splitsIntoSeparateRecords() {
        JulRelayHandler h = new JulRelayHandler();
        h.publish(record("x", Level.INFO, "line1\nline2\nline3"));

        assertEquals(3, emitted.size());
        assertEquals("line1", emitted.get(0)[3]);
        assertEquals("line2", emitted.get(1)[3]);
        assertEquals("line3", emitted.get(2)[3]);
    }

    @Test
    void publish_windowsLineEndings_splitsCorrectly() {
        JulRelayHandler h = new JulRelayHandler();
        h.publish(record("x", Level.INFO, "a\r\nb"));

        assertEquals(2, emitted.size());
        assertEquals("a", emitted.get(0)[3]);
        assertEquals("b", emitted.get(1)[3]);
    }

    // ── exception handling ────────────────────────────────────────────────────

    @Test
    void publish_withThrown_appendsStackTrace() {
        JulRelayHandler h = new JulRelayHandler();
        LogRecord r = record("x", Level.SEVERE, "oops");
        r.setThrown(new RuntimeException("boom"));
        h.publish(r);

        // First record is the message itself; subsequent records are stack-trace lines
        assertTrue(emitted.size() > 1, "Stack trace should produce multiple lines");
        assertEquals("oops", emitted.get(0)[3]);
        boolean hasStackLine = emitted.stream()
                .skip(1)
                .anyMatch(rec -> rec[3].contains("RuntimeException") || rec[3].contains("boom"));
        assertTrue(hasStackLine, "Stack trace lines must appear in relay output");
    }

    // ── j.u.l. Logger integration ─────────────────────────────────────────────

    @Test
    void installedInLogger_capturesLogCalls() {
        Logger logger = Logger.getLogger("test.baton.jul");
        JulRelayHandler h = new JulRelayHandler();
        logger.addHandler(h);
        logger.setUseParentHandlers(false);
        try {
            logger.info("from j.u.l.");
        } finally {
            logger.removeHandler(h);
            logger.setUseParentHandlers(true);
        }

        boolean found = emitted.stream().anyMatch(rec -> "from j.u.l.".equals(rec[3]));
        assertTrue(found, "Expected 'from j.u.l.' in relayed output");
    }

    // ── flush / close ─────────────────────────────────────────────────────────

    @Test
    void flushAndClose_doNotThrow() {
        JulRelayHandler h = new JulRelayHandler();
        assertDoesNotThrow(h::flush);
        assertDoesNotThrow(h::close);
    }
}
