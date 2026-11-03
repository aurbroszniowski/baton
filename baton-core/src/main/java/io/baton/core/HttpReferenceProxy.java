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

import io.baton.DistributedReference;
import io.baton.transport.HttpTransport;
import io.baton.transport.Serialization;

import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;

/**
 * HTTP-backed {@link DistributedReference} proxy.
 *
 * <p>Values are serialized to bytes before transmission and base64-encoded for
 * the GET response.  Suitable for JDK Serializable types and any classes
 * available on both the agent and orchestrator classpaths.
 */
public class HttpReferenceProxy<T extends Serializable> implements DistributedReference<T> {

    private static final long serialVersionUID = 1L;

    private final String baseUrl;
    private final String name;

    public HttpReferenceProxy(String baseUrl, String name) {
        this.baseUrl = baseUrl;
        this.name    = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
        try {
            String b64 = HttpTransport.get(baseUrl + "/primitive/reference/" + HttpTransport.encode(name) + "/get");
            byte[] bytes = Base64.getDecoder().decode(b64);
            return (T) Serialization.read(bytes);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Reference GET failed: " + name, e);
        }
    }

    @Override
    public void set(T value) {
        try {
            HttpTransport.post(baseUrl + "/primitive/reference/" + HttpTransport.encode(name) + "/set",
                    Serialization.write(value));
        } catch (IOException e) {
            throw new RuntimeException("Reference SET failed: " + name, e);
        }
    }

    @Override
    public boolean compareAndSet(T expect, T update) {
        try {
            String result = HttpTransport.post(
                    baseUrl + "/primitive/reference/" + HttpTransport.encode(name) + "/cas",
                    Serialization.write(expect, update));
            return Boolean.parseBoolean(result);
        } catch (IOException e) {
            throw new RuntimeException("Reference CAS failed: " + name, e);
        }
    }

}
