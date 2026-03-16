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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * A {@code java.util.logging} {@link Handler} that forwards log records to
 * the Baton relay via {@link BatonRuntime#emit}.
 *
 * <p>No external dependencies — {@code java.util.logging} ships with the JDK.
 * This handler can therefore live in {@code baton-agent} directly as a
 * built-in optional for frameworks that use j.u.l.
 *
 * <p>Install it from a {@link io.baton.RemoteInitializer}:
 * <pre>{@code
 * public class MyJulInitializer implements RemoteInitializer {
 *     {@literal @}Override
 *     public void initialize(NodeId agentId, Path workDir) {
 *         java.util.logging.Logger root =
 *             java.util.logging.LogManager.getLogManager().getLogger("");
 *         root.addHandler(new JulRelayHandler());
 *     }
 * }
 * }</pre>
 *
 * <p>Each log record is forwarded as a {@code "framework"} source record,
 * labelled with the j.u.l. logger name.  Multi-line messages (e.g. stack
 * traces appended by the caller) are split and submitted as separate records.
 * The current {@link io.baton.RemoteExecutionContext} provides the job ID
 * automatically — no explicit job reference is needed.
 */
public final class JulRelayHandler extends Handler {

    private final SimpleFormatter fmt = new SimpleFormatter();

    public JulRelayHandler() {}

    @Override
    public void publish(LogRecord record) {
        if (record == null || !isLoggable(record)) return;

        String loggerName = record.getLoggerName() != null ? record.getLoggerName() : "-";
        String message    = fmt.formatMessage(record);

        // Append exception if present
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            record.getThrown().printStackTrace(new PrintWriter(sw));
            message = message + "\n" + sw.toString().trim();
        }

        // Split multi-line text so each relay record contains exactly one line
        for (String line : message.split("\\r?\\n", -1)) {
            if (!line.isEmpty()) {
                BatonRuntime.emit("framework", loggerName, "-", line);
            }
        }
    }

    @Override public void flush() {}
    @Override public void close() {}
}
