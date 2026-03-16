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

import java.io.PrintStream;

/**
 * Default {@link LogSink} that writes structured lines to a {@link PrintStream}.
 *
 * <p>Output format:
 * <pre>
 * [{nodeTag}][{jobId}][{tag}] {line}
 * </pre>
 *
 * where {@code nodeTag} is the compact agent identifier
 * (e.g. {@code baton-agent-8700@host#pid}), {@code jobId} is the first 8 chars
 * of the job UUID (or {@code "-"} outside a job), and {@code tag} is the process
 * or logger label (or {@code "-"} for untagged system output).
 *
 * <p>ANSI colour is opt-in via the {@link #withAnsi()} factory method.
 * When enabled, the log line is coloured by source:
 * <ul>
 *   <li>{@code system-err} / {@code stderr} stream → red
 *   <li>{@code framework} → cyan
 *   <li>everything else → white (default terminal colour)
 * </ul>
 */
class PrintingLogSink implements LogSink {

    // ANSI escape codes
    private static final String RESET = "\u001B[0m";
    private static final String RED   = "\u001B[31m";
    private static final String CYAN  = "\u001B[36m";
    private static final String WHITE = "\u001B[37m";

    private final PrintStream out;
    private final boolean     ansi;

    PrintingLogSink(PrintStream out) {
        this(out, false);
    }

    private PrintingLogSink(PrintStream out, boolean ansi) {
        this.out  = out;
        this.ansi = ansi;
    }

    /** Returns a new sink that wraps the same stream with ANSI colouring enabled. */
    PrintingLogSink withAnsi() {
        return new PrintingLogSink(out, true);
    }

    @Override
    public void accept(LogRecord record) {
        String tag    = !"-".equals(record.label) ? record.label : "-";
        String dispId = abbreviate(record.jobId);
        String meta   = String.format("[%s][%s][%s] ", record.nodeId, dispId, tag);

        if (ansi) {
            String colour = colourFor(record);
            out.println(colour + meta + record.line + RESET);
        } else {
            out.println(meta + record.line);
        }
    }

    /** Shortens UUIDs (36 chars) to 8 chars for display; leaves shorter IDs unchanged. */
    static String abbreviate(String jobId) {
        return jobId != null && jobId.length() >= 32 ? jobId.substring(0, 8) : jobId;
    }

    private static String colourFor(LogRecord record) {
        if ("system-err".equals(record.source) || "stderr".equals(record.stream)) return RED;
        if ("framework".equals(record.source))                                    return CYAN;
        return WHITE;
    }
}
