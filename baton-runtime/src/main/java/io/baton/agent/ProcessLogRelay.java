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

import io.baton.RemoteExecutionContext;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads stdout and stderr of a {@link Process} and relays each line to a
 * {@link LogRelayClient}.
 *
 * <p>This class is Baton-generic: it attaches to any {@code Process}
 * regardless of what framework spawned it.  Framework or adapter code decides
 * when to call {@code new ProcessLogRelay(process, relay, jobId, label)}.
 * Baton does not inspect the process type in any way.
 *
 * <p>Usage:
 * <pre>{@code
 * Process p = new ProcessBuilder("...").start();
 * ProcessLogRelay relay = new ProcessLogRelay(p, logClient, jobId, "my-server");
 * // ... wait for process ...
 * relay.awaitEof(30, TimeUnit.SECONDS);
 * relay.close();
 * }</pre>
 */
public class ProcessLogRelay implements Closeable {

    private final Thread        stdoutThread;
    private final Thread        stderrThread;
    private final CountDownLatch latch = new CountDownLatch(2);

    /**
     * Convenience factory that attaches to a {@link Process} using the current
     * {@link RemoteExecutionContext} as the job identifier.
     *
     * <p>Framework code running inside a Baton job should prefer this factory over
     * the constructor, since it avoids the need to pass the job ID explicitly.
     * If no execution context is active, {@code "-"} is used as the job ID.
     *
     * @param process       the running child process
     * @param relay         the agent's shared {@link LogRelayClient}
     * @param processLabel  short label shown in log output (e.g. {@code "tc-server"})
     * @return a started {@link ProcessLogRelay}
     */
    public static ProcessLogRelay attach(Process process, LogRelayClient relay,
                                         String processLabel) {
        String jobId = RemoteExecutionContext.jobId();
        return new ProcessLogRelay(process, relay, jobId != null ? jobId : "-", processLabel);
    }

    /**
     * Starts two daemon threads that read stdout and stderr and submit each
     * line to {@code relay}.
     *
     * @param process       the running child process
     * @param relay         the agent's shared {@link LogRelayClient}
     * @param jobId         job identifier, or {@code "-"} if outside a job
     * @param processLabel  short label shown in log output (e.g. {@code "tc-server"})
     */
    public ProcessLogRelay(Process process, LogRelayClient relay,
                           String jobId, String processLabel) {
        this.stdoutThread = relayThread(
                process.getInputStream(), relay, jobId, processLabel,
                "stdout", "baton-relay-stdout-" + processLabel);
        this.stderrThread = relayThread(
                process.getErrorStream(), relay, jobId, processLabel,
                "stderr", "baton-relay-stderr-" + processLabel);
        stdoutThread.start();
        stderrThread.start();
    }

    /**
     * Blocks until both stdout and stderr streams reach EOF (process exited
     * and all buffered output has been submitted to the relay).
     *
     * @throws TimeoutException     if streams do not close within the given timeout
     * @throws InterruptedException if interrupted while waiting
     */
    public void awaitEof(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        if (!latch.await(timeout, unit)) {
            throw new TimeoutException(
                    "ProcessLogRelay: streams did not reach EOF within " + timeout + " " + unit);
        }
    }

    /**
     * Interrupts the relay threads.  Call on job cancellation or agent
     * shutdown.  Does not wait for threads to terminate.
     */
    @Override
    public void close() {
        stdoutThread.interrupt();
        stderrThread.interrupt();
    }

    // Internals 

    private Thread relayThread(InputStream stream, LogRelayClient relay,
                               String jobId, String label,
                               String streamName, String threadName) {
        Thread t = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    relay.submit(jobId, "process", label, streamName, line);
                }
            } catch (IOException e) {
                if (!Thread.currentThread().isInterrupted()) {
                    System.err.printf("[baton-process-relay] Error reading %s for '%s': %s%n",
                            streamName, label, e.getMessage());
                }
            } finally {
                latch.countDown();
            }
        }, threadName);
        t.setDaemon(true);
        return t;
    }
}
