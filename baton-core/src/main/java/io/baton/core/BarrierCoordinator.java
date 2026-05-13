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

import io.baton.DistributedBarrier;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Cyclic barrier living in the orchestrator JVM.
 *
 * <p>In Phase 1 (in-process) callers block directly in this JVM.
 * In Phase 2 (HTTP) the barrier HTTP handler calls {@link #arrive} and blocks
 * the HTTP server thread until all parties have arrived, then releases them all.
 *
 * <p>Protocol: each participant posts {@code (name, generation, nodeId)} and
 * blocks.  When the Nth party arrives the generation counter is incremented and
 * all waiters are released — exactly like {@link java.util.concurrent.CyclicBarrier}.
 */
class BarrierCoordinator {

    private final ConcurrentHashMap<String, BarrierState> barriers = new ConcurrentHashMap<>();

    /** Returns a {@link DistributedBarrier} proxy backed by this coordinator (in-process). */
    DistributedBarrier getOrCreate(String name, int parties) {
        BarrierState state = barriers.computeIfAbsent(name, k -> new BarrierState(parties));
        return new InProcessBarrier(state);
    }

    /**
     * Called by the HTTP handler (Phase 2) when an agent sends {@code POST /barrier/{name}/arrive}.
     * Blocks the calling (HTTP server) thread until all {@code count} parties have arrived.
     *
     * @param generation  the generation the caller thinks it is in (prevents stale requests)
     * @param timeoutMs   wall-clock deadline in milliseconds from now
     * @return arrival index (0 = first to arrive, parties-1 = last)
     */
    int arrive(String name, int generation, long timeoutMs) throws InterruptedException, TimeoutException {
        BarrierState state = barriers.get(name);
        if (state == null) throw new IllegalStateException("Unknown barrier: " + name);
        return doArrive(state, generation, System.currentTimeMillis() + timeoutMs);
    }

    private static int doArrive(BarrierState state, int generation, long deadlineMs)
            throws InterruptedException, TimeoutException {
        synchronized (state.lock) {
            // Wait if we are ahead of the current generation (stale request from previous cycle)
            while (state.generation != generation) {
                long remaining = deadlineMs - System.currentTimeMillis();
                if (remaining <= 0) throw new TimeoutException("Barrier stale generation timeout");
                state.lock.wait(remaining);
            }

            int myIndex = state.arrived++;
            if (state.arrived == state.parties) {
                // Last to arrive — release all waiters
                state.generation++;
                state.arrived = 0;
                state.lock.notifyAll();
            } else {
                // Wait for the generation to advance
                int expectedGen = generation + 1;
                while (state.generation < expectedGen) {
                    long remaining = deadlineMs - System.currentTimeMillis();
                    if (remaining <= 0) {
                        throw new TimeoutException(
                                "Barrier timeout: " + state.arrived + "/" + state.parties + " arrived");
                    }
                    state.lock.wait(remaining);
                }
            }
            return myIndex;
        }
    }

    // Inner types 

    private static final class BarrierState {
        final int    parties;
        int          generation = 0;
        int          arrived    = 0;
        final Object lock       = new Object();

        BarrierState(int parties) { this.parties = parties; }
    }

    private static final class InProcessBarrier implements DistributedBarrier {
        private static final long serialVersionUID = 1L;
        private final BarrierState state;

        InProcessBarrier(BarrierState state) { this.state = state; }

        @Override
        public int await() throws InterruptedException {
            try {
                return doArrive(state, state.generation, Long.MAX_VALUE);
            } catch (TimeoutException e) {
                throw new AssertionError("Unreachable", e);
            }
        }

        @Override
        public int await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            return doArrive(state, state.generation, System.currentTimeMillis() + unit.toMillis(timeout));
        }
    }
}
