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

/**
 * Immutable log record received from a remote agent.
 *
 * <p>Fields use {@code "-"} as a sentinel for "not applicable" or "unknown".
 */
public final class LogRecord {

    /** Compact agent log tag, e.g. {@code "baton-agent-8700@host#1234"}. */
    public final String nodeId;

    /**
     * Full job identifier (e.g. a UUID string), or {@code "-"} if the record was
     * produced outside a job context.  Display layers may abbreviate this for
     * readability, but the full value is preserved here for sink use (e.g. file naming).
     */
    public final String jobId;

    /**
     * Producer type: {@code "process"}, {@code "system-out"}, {@code "system-err"},
     * or {@code "framework"}.
     */
    public final String source;

    /** Process name or logger name, or {@code "-"} if not applicable. */
    public final String label;

    /** Stream within source: {@code "stdout"}, {@code "stderr"}, or {@code "-"}. */
    public final String stream;

    /** The log line text. */
    public final String line;

    public LogRecord(String nodeId, String jobId, String source,
                     String label, String stream, String line) {
        this.nodeId  = nodeId  != null && !nodeId.isEmpty()  ? nodeId  : "-";
        this.jobId   = jobId   != null && !jobId.isEmpty()   ? jobId   : "-";
        this.source  = source  != null && !source.isEmpty()  ? source  : "-";
        this.label   = label   != null && !label.isEmpty()   ? label   : "-";
        this.stream  = stream  != null && !stream.isEmpty()  ? stream  : "-";
        this.line    = line    != null                       ? line    : "";
    }
}
