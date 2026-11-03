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

import io.baton.ClassBundle;
import io.baton.RemoteCallable;
import io.baton.RemoteExecutionContext;
import io.baton.RemoteRunnable;
import io.baton.transport.HttpTransport;
import io.baton.transport.Serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Receives a serialized {@link ClassBundle} from the orchestrator, deserializes the
 * lambda using a {@link BundleClassLoader}, executes it, and POSTs the result (or
 * exception) back to the orchestrator.
 */
public class JobRunner {

    private volatile String orchestratorUrl;
    private final String agentRunId;
    private volatile String nodeId;
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "baton-job-runner");
        t.setDaemon(true);
        return t;
    });

    public JobRunner(String agentRunId, String nodeId) {
        this.agentRunId      = agentRunId;
        this.nodeId          = nodeId;
    }

    void connect(String orchestratorUrl) { this.orchestratorUrl = orchestratorUrl; }

    void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public Future<Void> submit(RemoteRunnable job) {
        return submit(() -> { job.run(); return null; });
    }

    public <R> Future<R> submit(RemoteCallable<R> job) {
        return pool.submit(() -> {
            RemoteExecutionContext.set(nodeId, UUID.randomUUID().toString());
            try { return job.call(); }
            finally { RemoteExecutionContext.clear(); }
        });
    }

    public void accept(byte[] payload) {
        pool.submit(() -> {
            String jobId = null;
            try {
                ClassBundle bundle;
                try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(payload))) {
                    jobId = in.readUTF();
                    bundle = (ClassBundle) in.readObject();
                }

                System.out.printf("[baton-agent][%s][%s] Running job%n", agentRunId, jobId);
                RemoteExecutionContext.set(nodeId, jobId);
                Object result;
                try {
                    result = execute(bundle);
                } finally {
                    RemoteExecutionContext.clear();
                }
                System.out.printf("[baton-agent][%s][%s] Job completed successfully%n", agentRunId, jobId);
                postResult(jobId, Serialization.write(result), false);
            } catch (Throwable t) {
                if (jobId != null) {
                    System.out.printf("[baton-agent][%s][%s] Job failed: %s%n", agentRunId, jobId, t.getMessage());
                    try { postResult(jobId, Serialization.write(t), true); } catch (Exception ignored) {}
                }
            }
        });
    }

    public void shutdown() {
        pool.shutdownNow();
    }

    private Object execute(ClassBundle bundle) throws Exception {
        BundleClassLoader loader = new BundleClassLoader(bundle.classes, getClass().getClassLoader());

        Object job;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bundle.lambda)) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
                try {
                    return loader.loadClass(desc.getName());
                } catch (ClassNotFoundException e) {
                    return super.resolveClass(desc);
                }
            }
        }) {
            job = ois.readObject();
        }
        if (job instanceof RemoteCallable) {
            return ((RemoteCallable<?>) job).call();
        } else if (job instanceof RemoteRunnable) {
            ((RemoteRunnable) job).run();
            return null;
        } else {
            throw new IllegalArgumentException("Unknown job type: " + job.getClass());
        }
    }

    private void postResult(String jobId, byte[] body, boolean isException) throws IOException {
        String url = orchestratorUrl + "/agent/result/" + jobId + "?exception=" + isException;
        HttpTransport.post(url, body);
    }
}
