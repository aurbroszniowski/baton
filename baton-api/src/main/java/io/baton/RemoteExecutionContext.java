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

/**
 * Thread-local execution context for Baton jobs.
 *
 * <p>The Baton job runner calls {@link #set} before invoking a job and
 * {@link #clear} in a {@code finally} block.  Any code running inside a Baton
 * job — including log relay utilities, child-process helpers, and optional
 * framework bridges — can read the current {@link #nodeId()} and
 * {@link #jobId()} to tag records without knowing the specific job.
 *
 * <p>Both accessors return {@code null} when called outside a job context
 * (e.g. from agent startup code or framework threads not managed by Baton).
 *
 * <p>Concurrent jobs running in separate threads keep completely independent
 * contexts because each thread has its own {@code ThreadLocal} slot.
 *
 * <p>This class lives in {@code baton-api} so it is accessible to both the
 * agent fat-JAR and the in-process {@code BatonAgentFabric} path in
 * {@code baton-core}.
 */
public final class RemoteExecutionContext {

    private static final ThreadLocal<Context> CURRENT = new ThreadLocal<>();

    private RemoteExecutionContext() {}

    /**
     * Set the execution context for the calling thread.
     * Called by the Baton job runner before job execution.
     */
    public static void set(String nodeId, String jobId) {
        CURRENT.set(new Context(nodeId, jobId));
    }

    /**
     * Clear the execution context for the calling thread.
     * Called by the Baton job runner in a {@code finally} block after job execution.
     */
    public static void clear() {
        CURRENT.remove();
    }

    /**
     * Returns the node ID of the current agent, or {@code null} if called
     * outside a Baton job context.
     */
    public static String nodeId() {
        Context c = CURRENT.get();
        return c != null ? c.nodeId : null;
    }

    /**
     * Returns the job ID of the currently executing job, or {@code null} if
     * called outside a Baton job context.
     */
    public static String jobId() {
        Context c = CURRENT.get();
        return c != null ? c.jobId : null;
    }

    // Internal 

    private static final class Context {
        final String nodeId;
        final String jobId;

        Context(String nodeId, String jobId) {
            this.nodeId = nodeId;
            this.jobId  = jobId;
        }
    }
}
