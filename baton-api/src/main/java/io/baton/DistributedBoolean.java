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

/**
 * A named boolean flag that is consistent across all nodes.
 * All state lives in the orchestrator JVM; agents reach it via HTTP.
 */
public interface DistributedBoolean extends Serializable {

    boolean get();
    void    set(boolean value);
    boolean getAndSet(boolean value);
    boolean compareAndSet(boolean expect, boolean update);
}
