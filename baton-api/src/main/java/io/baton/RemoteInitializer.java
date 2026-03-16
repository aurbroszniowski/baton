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
package io.baton;

import java.nio.file.Path;

/**
 * Called by the Baton agent once, after the HTTP server is ready and before
 * the agent accepts its first job.  Implementations are discovered via
 * {@link java.util.ServiceLoader}.
 *
 * <p>Use this hook to initialize any JVM-static state that your framework
 * requires on a remote agent — logging appenders, dependency-injection
 * containers, Angela's {@code AgentController}, etc.
 *
 * <p>Example — install a logback appender:
 * <pre>{@code
 * public class MyLogbackInitializer implements RemoteInitializer {
 *     {@literal @}Override
 *     public void initialize(NodeId agentId, Path workDir) {
 *         LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
 *         ctx.getLogger(Logger.ROOT_LOGGER_NAME)
 *            .addAppender(new LogbackRelayAppender());
 *     }
 * }
 * }</pre>
 *
 * Register the implementation in
 * {@code META-INF/services/io.baton.RemoteInitializer}.
 *
 * <p>Baton catches and logs any exception thrown by {@link #initialize}
 * so a misbehaving initializer never prevents the agent from starting.
 */
public interface RemoteInitializer {

    /**
     * Initialize framework state on the remote agent.
     *
     * @param agentId  the identity of this agent (name, hostname, port, pid)
     * @param workDir  a writable directory local to the agent that persists
     *                 for the agent's lifetime (e.g. {@code ~/baton/<name>})
     */
    void initialize(NodeId agentId, Path workDir);
}
