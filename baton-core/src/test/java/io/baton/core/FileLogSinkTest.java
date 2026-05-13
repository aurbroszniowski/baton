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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FileLogSink}.
 */
class FileLogSinkTest {

    private static LogRecord rec(String jobId, String source, String label, String line) {
        return new LogRecord("baton-agent-0@host#1", jobId, source, label, "-", line);
    }

    // file creation 

    @Test
    void createsLogsDirLazily(@TempDir Path root) throws IOException {
        Path logsDir = root.resolve("logs");
        assertFalse(Files.exists(logsDir), "logs/ must not exist before first record");

        try (FileLogSink sink = new FileLogSink(root)) {
            sink.accept(rec("abc12345", "system-out", "-", "hello"));
        }

        assertTrue(Files.exists(logsDir), "logs/ must be created on first accept()");
    }

    @Test
    void createsOneFilePerJob(@TempDir Path root) throws IOException {
        try (FileLogSink sink = new FileLogSink(root)) {
            sink.accept(rec("job-A", "system-out", "-", "line from A"));
            sink.accept(rec("job-B", "system-out", "-", "line from B"));
        }

        Path logsDir = root.resolve("logs");
        assertTrue(Files.exists(logsDir.resolve("job-A.log")), "job-A.log must exist");
        assertTrue(Files.exists(logsDir.resolve("job-B.log")), "job-B.log must exist");
    }

    @Test
    void noJobId_writesToDashLog(@TempDir Path root) throws IOException {
        try (FileLogSink sink = new FileLogSink(root)) {
            sink.accept(rec("-", "system-out", "-", "outside a job"));
        }

        assertTrue(Files.exists(root.resolve("logs").resolve("-.log")));
    }

    // content 

    @Test
    void multipleLinesGoToSameFile(@TempDir Path root) throws IOException {
        try (FileLogSink sink = new FileLogSink(root)) {
            sink.accept(rec("j1", "system-out", "-",  "first"));
            sink.accept(rec("j1", "system-out", "-",  "second"));
            sink.accept(rec("j1", "system-out", "-",  "third"));
        }

        List<String> lines = Files.readAllLines(root.resolve("logs/j1.log"));
        assertEquals(3, lines.size());
        assertTrue(lines.get(0).contains("first"));
        assertTrue(lines.get(1).contains("second"));
        assertTrue(lines.get(2).contains("third"));
    }

    @Test
    void lineContainsNodeAndJobAndLabel(@TempDir Path root) throws IOException {
        try (FileLogSink sink = new FileLogSink(root)) {
            sink.accept(new LogRecord("baton-agent-8700@host#42", "deadbeef",
                    "process", "tc-server", "stderr", "server started"));
        }

        String content = Files.readString(root.resolve("logs/deadbeef.log"));
        assertTrue(content.contains("baton-agent-8700@host#42"), "nodeId must appear in line");
        assertTrue(content.contains("deadbeef"),                  "jobId must appear in line");
        assertTrue(content.contains("tc-server"),                 "label must appear in line");
        assertTrue(content.contains("server started"),            "log line must appear");
    }

    @Test
    void labelDash_showsDashInFile(@TempDir Path root) throws IOException {
        try (FileLogSink sink = new FileLogSink(root)) {
            sink.accept(rec("j1", "system-out", "-", "plain output"));
        }

        String content = Files.readString(root.resolve("logs/j1.log"));
        assertTrue(content.contains("[-]"), "dash label must appear as [-]");
    }

    // path traversal rejection (P1) 

    @Test
    void logFileFor_rejectsPathTraversal(@TempDir Path root) {
        FileLogSink sink = new FileLogSink(root);
        assertThrows(IllegalArgumentException.class, () -> sink.logFileFor("../../outside"));
    }

    @Test
    void logFileFor_rejectsDots(@TempDir Path root) {
        FileLogSink sink = new FileLogSink(root);
        assertThrows(IllegalArgumentException.class, () -> sink.logFileFor("some.evil"));
    }

    @Test
    void logFileFor_rejectsSlash(@TempDir Path root) {
        FileLogSink sink = new FileLogSink(root);
        assertThrows(IllegalArgumentException.class, () -> sink.logFileFor("sub/dir"));
    }

    @Test
    void accept_unsafeJobId_recordIsDroppedNotWritten(@TempDir Path root) {
        FileLogSink sink = new FileLogSink(root);
        // Must not throw and must not create any file outside rootDir
        assertDoesNotThrow(() ->
                sink.accept(rec("../../evil", "system-out", "-", "should not appear")));

        assertFalse(Files.exists(root.getParent().resolve("evil.log")),
                "Path traversal must not create files outside rootDir");
    }

    @Test
    void accept_uuidJobId_usesFullIdAsFilename(@TempDir Path root) throws IOException {
        // UUIDs contain dashes, which are in the allowlist — full UUID must be the filename
        String uuid = "73243b5e-2a8e-453c-bc08-ded0bda5d0ed";
        try (FileLogSink sink = new FileLogSink(root)) {
            sink.accept(rec(uuid, "system-out", "-", "a line"));
        }
        assertTrue(Files.exists(root.resolve("logs/" + uuid + ".log")),
                "Full UUID must be used as filename (no truncation)");
    }

    // logFileFor 

    @Test
    void logFileFor_returnsExpectedPath(@TempDir Path root) {
        FileLogSink sink = new FileLogSink(root);
        assertEquals(root.resolve("logs/abc.log"), sink.logFileFor("abc"));
    }

    // close 

    @Test
    void close_isIdempotent(@TempDir Path root) throws IOException {
        FileLogSink sink = new FileLogSink(root);
        sink.accept(rec("j1", "system-out", "-", "a line"));
        assertDoesNotThrow(() -> {
            sink.close();
            sink.close(); // second close must not throw
        });
    }

    @Test
    void close_flushesContent(@TempDir Path root) throws IOException {
        FileLogSink sink = new FileLogSink(root);
        sink.accept(rec("flush-job", "system-out", "-", "important line"));
        sink.close();

        // After close, the file must contain the line
        String content = Files.readString(root.resolve("logs/flush-job.log"));
        assertTrue(content.contains("important line"), "Content must be flushed after close()");
    }

    // concurrent writes 

    @Test
    void concurrentWrites_doNotCorruptFile(@TempDir Path root) throws Exception {
        int    threads = 8;
        int    linesEach = 50;
        String jobId = "concurrent-job";

        try (FileLogSink sink = new FileLogSink(root)) {
            Thread[] workers = new Thread[threads];
            for (int t = 0; t < threads; t++) {
                final int idx = t;
                workers[t] = new Thread(() -> {
                    for (int i = 0; i < linesEach; i++) {
                        sink.accept(rec(jobId, "system-out", "-", "thread-" + idx + "-line-" + i));
                    }
                });
            }
            for (Thread w : workers) w.start();
            for (Thread w : workers) w.join(5_000);
        }

        List<String> lines = Files.readAllLines(root.resolve("logs/" + jobId + ".log"));
        assertEquals(threads * linesEach, lines.size(),
                "All " + (threads * linesEach) + " lines must be written without loss");
    }
}
