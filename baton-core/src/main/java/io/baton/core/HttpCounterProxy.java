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

import io.baton.DistributedCounter;

import java.io.IOException;

/**
 * HTTP-backed {@link DistributedCounter} proxy.
 *
 * <p>Carries the orchestrator base URL and counter name.  When captured in a
 * lambda and shipped to an agent via {@link ClassBundle}, every operation
 * calls back to the orchestrator over HTTP, so all agents share the same
 * counter state.
 */
public class HttpCounterProxy implements DistributedCounter {

    private static final long serialVersionUID = 1L;

    private final String baseUrl;
    private final String name;

    public HttpCounterProxy(String baseUrl, String name) {
        this.baseUrl = baseUrl;
        this.name    = name;
    }

    @Override
    public long get() {
        return doLong("/primitive/counter/" + name + "/get");
    }

    @Override
    public long incrementAndGet() {
        return doLong("/primitive/counter/" + name + "/increment");
    }

    @Override
    public long getAndIncrement() {
        return doLong("/primitive/counter/" + name + "/getAndIncrement");
    }

    @Override
    public long getAndSet(long value) {
        return doLong("/primitive/counter/" + name + "/getAndSet?value=" + value);
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return doBool("/primitive/counter/" + name + "/cas?expect=" + expect + "&update=" + update);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private long doLong(String path) {
        try {
            return Long.parseLong(OrchestratorHttp.post(baseUrl + path));
        } catch (IOException e) {
            throw new RuntimeException("Counter HTTP call failed: " + path, e);
        }
    }

    private boolean doBool(String path) {
        try {
            return Boolean.parseBoolean(OrchestratorHttp.post(baseUrl + path));
        } catch (IOException e) {
            throw new RuntimeException("Counter HTTP call failed: " + path, e);
        }
    }
}
