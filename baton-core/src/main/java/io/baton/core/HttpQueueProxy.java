/*
 * Copyright Aurelien Broszniowski
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

import io.baton.DistributedQueue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * HTTP-backed {@link DistributedQueue} proxy.
 *
 * <p>Items are Java-serialized before being sent to the orchestrator's
 * {@code /transfer/{name}/push} endpoint and deserialized on retrieval from
 * {@code /transfer/{name}/pop}.  The queue is backed by the orchestrator's
 * in-memory {@link QueueStore}.
 */
public class HttpQueueProxy<T extends Serializable> implements DistributedQueue<T> {

    private static final long serialVersionUID = 1L;

    /** Timeout used for {@link #take()} — effectively unlimited (24 hours). */
    private static final long TAKE_TIMEOUT_MS = 86_400_000L;

    private final String baseUrl;
    private final String name;

    public HttpQueueProxy(String baseUrl, String name) {
        this.baseUrl = baseUrl;
        this.name    = name;
    }

    @Override
    public void put(T item) throws InterruptedException {
        try {
            OrchestratorHttp.post(baseUrl + "/transfer/" + name + "/push", serialize(item));
        } catch (IOException e) {
            throw new RuntimeException("Queue PUT failed: " + name, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T take() throws InterruptedException {
        try {
            byte[] bytes = OrchestratorHttp.getBytes(
                    baseUrl + "/transfer/" + name + "/pop?timeoutMs=" + TAKE_TIMEOUT_MS,
                    (int) (TAKE_TIMEOUT_MS + 10_000L));
            if (bytes == null) throw new InterruptedException("Queue take returned no item: " + name);
            return (T) deserialize(bytes);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Queue TAKE failed: " + name, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long ms = unit.toMillis(timeout);
        try {
            byte[] bytes = OrchestratorHttp.getBytes(
                    baseUrl + "/transfer/" + name + "/pop?timeoutMs=" + ms,
                    (int) Math.min(ms + 10_000L, Integer.MAX_VALUE));
            if (bytes == null) return null;
            return (T) deserialize(bytes);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Queue POLL failed: " + name, e);
        }
    }

    // Helpers 

    private byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(buf)) {
            oos.writeObject(obj);
        }
        return buf.toByteArray();
    }

    private Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        }
    }
}
