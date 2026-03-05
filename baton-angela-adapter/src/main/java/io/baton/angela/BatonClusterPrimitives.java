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
package io.baton.angela;

import io.baton.Fabric;
import org.terracotta.angela.agent.com.grid.ClusterPrimitives;
import org.terracotta.angela.common.cluster.AtomicBoolean;
import org.terracotta.angela.common.cluster.AtomicCounter;
import org.terracotta.angela.common.cluster.AtomicReference;
import org.terracotta.angela.common.cluster.Barrier;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Bridges Angela's {@link ClusterPrimitives} to Baton's {@link Fabric} primitives.
 */
class BatonClusterPrimitives implements ClusterPrimitives {

    private final Fabric fabric;

    BatonClusterPrimitives(Fabric fabric) {
        this.fabric = fabric;
    }

    @Override
    public AtomicCounter atomicCounter(String name, long initialValue) {
        io.baton.DistributedCounter dc = fabric.counter(name, initialValue);
        return new AtomicCounter() {
            @Override public long incrementAndGet()                 { return dc.incrementAndGet(); }
            @Override public long getAndIncrement()                 { return dc.getAndIncrement(); }
            @Override public long get()                             { return dc.get(); }
            @Override public long getAndSet(long v)                 { return dc.getAndSet(v); }
            @Override public boolean compareAndSet(long e, long u)  { return dc.compareAndSet(e, u); }
        };
    }

    @Override
    public AtomicBoolean atomicBoolean(String name, boolean initialValue) {
        io.baton.DistributedBoolean db = fabric.bool(name, initialValue);
        return new AtomicBoolean() {
            @Override public boolean get()                              { return db.get(); }
            @Override public void    set(boolean v)                     { db.set(v); }
            @Override public boolean getAndSet(boolean v)               { return db.getAndSet(v); }
            @Override public boolean compareAndSet(boolean e, boolean u){ return db.compareAndSet(e, u); }
        };
    }

    @Override
    public <T> AtomicReference<T> atomicReference(String name, T initialValue) {
        @SuppressWarnings("unchecked")
        io.baton.DistributedReference<java.io.Serializable> dr =
                fabric.reference(name, (java.io.Serializable) initialValue);
        return new AtomicReference<T>() {
            @Override @SuppressWarnings("unchecked")
            public T       get()                   { return (T) dr.get(); }
            @Override public void    set(T v)       { dr.set((java.io.Serializable) v); }
            @Override public boolean compareAndSet(T e, T u) {
                return dr.compareAndSet((java.io.Serializable) e, (java.io.Serializable) u);
            }
        };
    }

    @Override
    public Barrier barrier(String name, int parties) {
        io.baton.DistributedBarrier db = fabric.barrier(name, parties);
        return new Barrier() {
            @Override public int await() {
                try { return db.await(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e); }
            }
            @Override public int await(long timeout, TimeUnit unit) throws TimeoutException {
                try { return db.await(timeout, unit); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e); }
            }
        };
    }
}
