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

import java.util.function.UnaryOperator;

/**
 * Receives {@link LogRecord}s from the HTTP endpoint and dispatches them to
 * the configured {@link LogSink}.
 *
 * <p>Sink exceptions are caught here and printed locally; they never propagate
 * to the HTTP handler or affect job execution.
 */
class LogCollector {

    private volatile LogSink sink;

    LogCollector(LogSink sink) {
        this.sink = sink;
    }

    /** Replace the active sink at runtime. The swap is immediately visible to all threads. */
    void setSink(LogSink sink) {
        this.sink = sink;
    }

    /** Apply a transformation to the current sink (e.g. wrap with colour support). */
    void mapSink(UnaryOperator<LogSink> fn) {
        this.sink = fn.apply(this.sink);
    }

    /** Dispatch a record to the active sink. Best-effort: exceptions are swallowed. */
    void collect(LogRecord record) {
        LogSink s = this.sink;
        if (s == null) return;
        try {
            s.accept(record);
        } catch (Exception e) {
            System.err.printf("[baton-log-collector] sink threw: %s%n", e.getMessage());
        }
    }
}
