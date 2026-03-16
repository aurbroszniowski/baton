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
 * Consumer of {@link LogRecord}s collected from remote agents.
 *
 * <p>Implementations must be thread-safe.  Exceptions thrown by
 * {@link #accept} are caught by {@link LogCollector} and logged locally;
 * they never propagate back to the agent or fail the job.
 */
@FunctionalInterface
public interface LogSink {

    void accept(LogRecord record);
}
