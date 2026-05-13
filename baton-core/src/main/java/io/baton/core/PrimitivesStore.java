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

import io.baton.DistributedBoolean;
import io.baton.DistributedCounter;
import io.baton.DistributedReference;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe, in-memory store for all distributed primitive values.
 *
 * <p>In Phase 1 (in-process mode) the returned proxy objects access shared
 * state directly.  In Phase 2 (HTTP mode) agents will use HTTP-backed proxies
 * that call back to the orchestrator's {@link BatonServer}; those are
 * constructed by {@link HttpPrimitiveProxyFactory} (TODO Phase 2).
 */
class PrimitivesStore {

    private final ConcurrentHashMap<String, AtomicLong>            counters    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong>            booleans    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicReference<Object>> references = new ConcurrentHashMap<>();

    DistributedCounter getOrCreateCounter(String name, long initialValue) {
        AtomicLong atom = counters.computeIfAbsent(name, k -> new AtomicLong(initialValue));
        return new LocalCounter(atom);
    }

    DistributedBoolean getOrCreateBoolean(String name, boolean initialValue) {
        AtomicLong atom = booleans.computeIfAbsent(name, k -> new AtomicLong(initialValue ? 1L : 0L));
        return new LocalBoolean(atom);
    }

    @SuppressWarnings("unchecked")
    <T extends Serializable> DistributedReference<T> getOrCreateReference(String name, T initialValue) {
        AtomicReference<Object> atom = references.computeIfAbsent(name, k -> new AtomicReference<>(initialValue));
        return new LocalReference<>(atom);
    }

    // Named inner classes (serializable so they survive lambda capture) 

    private static final class LocalCounter implements DistributedCounter {
        private static final long serialVersionUID = 1L;
        private final AtomicLong atom;
        LocalCounter(AtomicLong atom) { this.atom = atom; }
        @Override public long incrementAndGet()                        { return atom.incrementAndGet(); }
        @Override public long getAndIncrement()                        { return atom.getAndIncrement(); }
        @Override public long get()                                    { return atom.get(); }
        @Override public long getAndSet(long v)                        { return atom.getAndSet(v); }
        @Override public boolean compareAndSet(long e, long u)         { return atom.compareAndSet(e, u); }
    }

    private static final class LocalBoolean implements DistributedBoolean {
        private static final long serialVersionUID = 1L;
        private final AtomicLong atom;
        LocalBoolean(AtomicLong atom) { this.atom = atom; }
        @Override public boolean get()                                 { return atom.get() != 0L; }
        @Override public void    set(boolean v)                        { atom.set(v ? 1L : 0L); }
        @Override public boolean getAndSet(boolean v)                  { return atom.getAndSet(v ? 1L : 0L) != 0L; }
        @Override public boolean compareAndSet(boolean e, boolean u)   { return atom.compareAndSet(e ? 1L : 0L, u ? 1L : 0L); }
    }

    private static final class LocalReference<T extends Serializable> implements DistributedReference<T> {
        private static final long serialVersionUID = 1L;
        private final AtomicReference<Object> atom;
        LocalReference(AtomicReference<Object> atom) { this.atom = atom; }
        @Override @SuppressWarnings("unchecked") public T get()        { return (T) atom.get(); }
        @Override public void    set(T v)                              { atom.set(v); }
        @Override public boolean compareAndSet(T e, T u) {
            // Use equals semantics: AtomicReference.compareAndSet uses ==, which
            // breaks for deserialized objects (e.g. values arriving via HTTP).
            while (true) {
                Object current = atom.get();
                if (!Objects.equals(current, e)) return false;
                if (atom.compareAndSet(current, u)) return true;
                // current was equal to e but changed between get() and CAS — retry
            }
        }
    }
}
