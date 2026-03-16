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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * A {@link LogSink} that writes each job's log lines to a dedicated file under
 * {@code <rootDir>/logs/<jobId>.log}.
 *
 * <p>Files are created lazily on the first line received for a job and kept
 * open until {@link #close()} is called.  Lines outside any job
 * ({@code jobId == "-"}) go to {@code <rootDir>/logs/-.log}.
 *
 * <p>Call {@link #close()} when the fabric shuts down to flush and close all
 * open writers.  The sink is safe to call from multiple threads.
 *
 * <p>Example:
 * <pre>{@code
 * fabric.setLogSink(new FileLogSink(Path.of("build/baton-logs")));
 * }</pre>
 */
public class FileLogSink implements LogSink, Closeable {

    /**
     * Allowlist for characters permitted in a log file name.
     * Anything outside {@code [a-zA-Z0-9_-]} is rejected to prevent path traversal.
     */
    private static final Pattern SAFE_ID = Pattern.compile("[a-zA-Z0-9_-]+");

    private final Path rootDir;
    private final ConcurrentMap<String, BufferedWriter> writers = new ConcurrentHashMap<>();

    /**
     * @param rootDir base directory; {@code <rootDir>/logs/} will be created if absent
     */
    public FileLogSink(Path rootDir) {
        this.rootDir = rootDir.resolve("logs");
    }

    @Override
    public void accept(LogRecord record) {
        String jobId = record.jobId;
        BufferedWriter w;
        try {
            w = writers.computeIfAbsent(jobId, this::openWriter);
        } catch (IllegalArgumentException e) {
            System.err.printf("[baton-file-log] Dropping record with unsafe jobId '%s': %s%n",
                    jobId, e.getMessage());
            return;
        }
        if (w == null) return; // open failed — already logged

        String tag  = !"-".equals(record.label) ? record.label : "-";
        String line = String.format("[%s][%s][%s] %s%n",
                record.nodeId, jobId, tag, record.line);
        try {
            synchronized (w) {
                w.write(line);
                w.flush();
            }
        } catch (IOException e) {
            System.err.printf("[baton-file-log] Failed to write to log for job %s: %s%n",
                    jobId, e.getMessage());
        }
    }

    /**
     * Returns the path that would be used for the given job's log file.
     * Throws {@link IllegalArgumentException} if {@code jobId} contains characters
     * outside {@code [a-zA-Z0-9_-]} (which would allow path traversal).
     */
    public Path logFileFor(String jobId) {
        if (!SAFE_ID.matcher(jobId).matches()) {
            throw new IllegalArgumentException(
                    "Unsafe jobId rejected (only [a-zA-Z0-9_-] allowed): " + jobId);
        }
        return rootDir.resolve(jobId + ".log");
    }

    @Override
    public void close() {
        writers.forEach((jobId, w) -> {
            try { synchronized (w) { w.close(); } }
            catch (IOException e) {
                System.err.printf("[baton-file-log] Failed to close log for job %s: %s%n",
                        jobId, e.getMessage());
            }
        });
        writers.clear();
    }

    // ── internals ─────────────────────────────────────────────────────────────

    private BufferedWriter openWriter(String jobId) {
        try {
            Files.createDirectories(rootDir);
            Path file = logFileFor(jobId);
            return Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.printf("[baton-file-log] Cannot open log file for job %s: %s%n",
                    jobId, e.getMessage());
            return null;
        }
    }
}
