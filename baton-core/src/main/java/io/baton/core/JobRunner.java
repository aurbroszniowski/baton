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

import io.baton.ClassBundle;
import io.baton.RemoteCallable;
import io.baton.RemoteExecutionContext;
import io.baton.RemoteRunnable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Receives a serialized {@link ClassBundle} from the orchestrator, deserializes the
 * lambda using a {@link BundleClassLoader}, executes it, and POSTs the result (or
 * exception) back to the orchestrator.
 */
public class JobRunner {

    private final String orchestratorUrl;
    private final String agentRunId;
    private final String nodeId;
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "baton-job-runner");
        t.setDaemon(true);
        return t;
    });

    public JobRunner(String orchestratorUrl, String agentRunId, String nodeId) {
        this.orchestratorUrl = orchestratorUrl;
        this.agentRunId      = agentRunId;
        this.nodeId          = nodeId;
    }

    /**
     * Accepts the raw HTTP body from {@code POST /job}, deserializes the bundle,
     * runs the lambda asynchronously, and sends the result back.
     */
    public void accept(byte[] payload) {
        pool.submit(() -> {
            String jobId = null;
            try {
                // Parse payload: jobId (UTF) + ClassBundle (object)
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload));
                jobId = ois.readUTF();
                ClassBundle bundle = (ClassBundle) ois.readObject();

                System.out.printf("[baton-agent][%s][%s] Running job%n", agentRunId, jobId);
                RemoteExecutionContext.set(nodeId, jobId);
                Object result;
                try {
                    result = execute(bundle);
                } finally {
                    RemoteExecutionContext.clear();
                }
                System.out.printf("[baton-agent][%s][%s] Job completed successfully%n", agentRunId, jobId);
                postResult(jobId, serialize(result), false);
            } catch (Throwable t) {
                if (jobId != null) {
                    System.out.printf("[baton-agent][%s][%s] Job failed: %s%n", agentRunId, jobId, t.getMessage());
                    try { postResult(jobId, serialize(t), true); } catch (Exception ignored) {}
                }
            }
        });
    }

    public void shutdown() {
        pool.shutdownNow();
    }

    // Private helpers 

    private Object execute(ClassBundle bundle) throws Exception {
        BundleClassLoader loader = new BundleClassLoader(bundle.classes, getClass().getClassLoader());

        // Deserialize lambda using our custom classloader so synthetic classes resolve
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bundle.lambda)) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
                try {
                    return loader.loadClass(desc.getName());
                } catch (ClassNotFoundException e) {
                    return super.resolveClass(desc);
                }
            }
        };

        Object job = ois.readObject();
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
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.getOutputStream().write(body);
        conn.getResponseCode(); // ensure the request is sent
    }

    private byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(buf)) {
            oos.writeObject(obj);
        }
        return buf.toByteArray();
    }
}
