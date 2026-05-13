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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP-backed {@link DistributedBarrier} proxy.
 *
 * <p>Each {@link #await()} call blocks an HTTP connection to the orchestrator's
 * {@code POST /barrier/{name}/arrive} endpoint until all parties have arrived
 * (long-poll).  The orchestrator's {@link BarrierCoordinator} is the
 * single source of truth.
 *
 * <p>The proxy tracks its local generation counter so that cyclic barrier
 * use works correctly: after each release the generation advances by one.
 */
public class HttpBarrierProxy implements DistributedBarrier {

    private static final long serialVersionUID = 1L;

    /** Wait up to 24 hours when no explicit timeout is given. */
    private static final long DEFAULT_TIMEOUT_MS = 86_400_000L;

    private final String baseUrl;
    private final String name;
    private final int    parties;

    /** Local generation counter — incremented after each barrier release. */
    private final AtomicInteger generation = new AtomicInteger(0);

    public HttpBarrierProxy(String baseUrl, String name, int parties) {
        this.baseUrl  = baseUrl;
        this.name     = name;
        this.parties  = parties;
    }

    @Override
    public int await() throws InterruptedException {
        try {
            return doArrive(DEFAULT_TIMEOUT_MS);
        } catch (TimeoutException e) {
            throw new AssertionError("Unreachable — default timeout is 24 hours", e);
        }
    }

    @Override
    public int await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        return doArrive(unit.toMillis(timeout));
    }

    // Private 

    private int doArrive(long timeoutMs) throws InterruptedException, TimeoutException {
        int gen = generation.get();
        String url = baseUrl + "/barrier/" + name + "/arrive"
                + "?count=" + parties
                + "&generation=" + gen
                + "&timeoutMs=" + timeoutMs;

        try {
            // The server holds the connection until all parties arrive; set
            // the client read-timeout slightly longer so we see the HTTP 408
            // response rather than a socket timeout.
            int readTimeout = (int) Math.min(timeoutMs + 10_000L, Integer.MAX_VALUE);
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setReadTimeout(readTimeout);
            conn.getOutputStream().close();

            int code = conn.getResponseCode();
            if (code == 408) {
                String detail = "";
                try {
                    InputStream err = conn.getErrorStream();
                    if (err != null) detail = new String(err.readAllBytes(), StandardCharsets.UTF_8).trim();
                } catch (IOException ignored) {}
                throw new TimeoutException(detail.isEmpty() ? "Barrier await timed out: " + name : detail);
            }
            if (code != 200) throw new IOException("HTTP " + code + " from barrier: " + name);

            String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int index = parseField(body, "index");
            // Advance generation: only one thread wins the CAS, others are no-ops
            generation.compareAndSet(gen, gen + 1);
            return index;
        } catch (SocketTimeoutException e) {
            throw new TimeoutException("Barrier await timed out (socket): " + name);
        } catch (IOException e) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            throw new RuntimeException("Barrier HTTP call failed: " + name, e);
        }
    }

    /** Parse {@code "field":N} from a minimal JSON object like {@code {"index":1,"generation":0}}. */
    private static int parseField(String json, String field) {
        String key = "\"" + field + "\":";
        int start = json.indexOf(key);
        if (start < 0) throw new IllegalArgumentException("Field '" + field + "' not found in: " + json);
        start += key.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) end++;
        return Integer.parseInt(json.substring(start, end));
    }
}
