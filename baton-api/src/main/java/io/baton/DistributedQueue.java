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
package io.baton;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * A named, unbounded blocking queue shared across all nodes.
 * Items are serialized for cross-JVM transfer.
 *
 * @param <T> element type — must be {@link Serializable}
 */
public interface DistributedQueue<T extends Serializable> extends Serializable {

    void put(T item) throws InterruptedException;
    T    take() throws InterruptedException;
    T    poll(long timeout, TimeUnit unit) throws InterruptedException;
}
