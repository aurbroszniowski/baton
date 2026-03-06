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

import java.io.IOException;

/**
 * HTTP-backed {@link DistributedBoolean} proxy.
 *
 * <p>Carries the orchestrator base URL and boolean name.  Serializable so it
 * can be captured in a lambda shipped to a remote agent.
 */
public class HttpBooleanProxy implements DistributedBoolean {

    private static final long serialVersionUID = 1L;

    private final String baseUrl;
    private final String name;

    public HttpBooleanProxy(String baseUrl, String name) {
        this.baseUrl = baseUrl;
        this.name    = name;
    }

    @Override
    public boolean get() {
        return doBool("/primitive/boolean/" + name + "/get");
    }

    @Override
    public void set(boolean value) {
        doPost("/primitive/boolean/" + name + "/set?value=" + value);
    }

    @Override
    public boolean getAndSet(boolean value) {
        return doBool("/primitive/boolean/" + name + "/getAndSet?value=" + value);
    }

    @Override
    public boolean compareAndSet(boolean expect, boolean update) {
        return doBool("/primitive/boolean/" + name + "/cas?expect=" + expect + "&update=" + update);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private boolean doBool(String path) {
        try {
            return Boolean.parseBoolean(OrchestratorHttp.post(baseUrl + path));
        } catch (IOException e) {
            throw new RuntimeException("Boolean HTTP call failed: " + path, e);
        }
    }

    private void doPost(String path) {
        try {
            OrchestratorHttp.post(baseUrl + path);
        } catch (IOException e) {
            throw new RuntimeException("Boolean HTTP call failed: " + path, e);
        }
    }
}
