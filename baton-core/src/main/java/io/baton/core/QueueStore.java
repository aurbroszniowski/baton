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

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * In-memory unbounded blocking queues, keyed by name.
 * Lives entirely in the orchestrator JVM.
 */
class QueueStore {

    private final ConcurrentHashMap<String, LinkedBlockingQueue<Object>> queues = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    <T extends Serializable> DistributedQueue<T> getOrCreate(String name) {
        LinkedBlockingQueue<Object> q = queues.computeIfAbsent(name, k -> new LinkedBlockingQueue<>());
        return new LocalQueue<>(q);
    }

    private static final class LocalQueue<T extends Serializable> implements DistributedQueue<T> {
        private static final long serialVersionUID = 1L;
        private final LinkedBlockingQueue<Object> q;

        LocalQueue(LinkedBlockingQueue<Object> q) { this.q = q; }

        @Override public void put(T item) throws InterruptedException     { q.put(item); }
        @Override @SuppressWarnings("unchecked")
        public T take() throws InterruptedException                       { return (T) q.take(); }
        @Override @SuppressWarnings("unchecked")
        public T poll(long timeout, TimeUnit unit) throws InterruptedException { return (T) q.poll(timeout, unit); }
    }
}
