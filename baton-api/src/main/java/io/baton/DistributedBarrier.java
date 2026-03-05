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
import java.util.concurrent.TimeoutException;

/**
 * A cyclic barrier that synchronises N nodes.
 *
 * <p>Implemented as a long-poll HTTP call to the orchestrator.  All threads
 * block at {@link #await} until every party has arrived, then are released
 * together.  The barrier resets automatically for the next cycle.
 */
public interface DistributedBarrier extends Serializable {

    /** Blocks until all parties have called {@code await}. Returns this thread's arrival index (0-based). */
    int await() throws InterruptedException;

    /** Same as {@link #await()} but with a timeout. */
    int await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;
}
