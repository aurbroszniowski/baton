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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Shared HTTP utilities used by the HTTP-backed proxy classes
 * ({@link HttpCounterProxy}, {@link HttpBooleanProxy}, etc.).
 *
 * <p>All methods throw {@link IOException} on non-200 responses.
 */
class OrchestratorHttp {

    private OrchestratorHttp() {}

    /** POST with no body; returns response body as a UTF-8 string. */
    static String post(String url) throws IOException {
        HttpURLConnection conn = open(url, "POST");
        conn.setDoOutput(true);
        conn.getOutputStream().close();
        return readText(conn);
    }

    /** POST with a binary body; returns response body as a UTF-8 string. */
    static String post(String url, byte[] body) throws IOException {
        HttpURLConnection conn = open(url, "POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.getOutputStream().write(body);
        return readText(conn);
    }

    /** GET; returns response body as a UTF-8 string. */
    static String get(String url) throws IOException {
        HttpURLConnection conn = open(url, "GET");
        return readText(conn);
    }

    /**
     * GET with an explicit read-timeout; returns the raw response bytes.
     * Returns {@code null} if the server responds 204 (No Content).
     */
    static byte[] getBytes(String url, int readTimeoutMs) throws IOException {
        HttpURLConnection conn = open(url, "GET");
        conn.setReadTimeout(readTimeoutMs);
        int code = conn.getResponseCode();
        if (code == 204) return null;
        if (code != 200) throw new IOException("HTTP " + code + " from " + url);
        return readBytes(conn.getInputStream());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static HttpURLConnection open(String url, String method) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        return conn;
    }

    private static String readText(HttpURLConnection conn) throws IOException {
        int code = conn.getResponseCode();
        if (code != 200) throw new IOException("HTTP " + code + " from " + conn.getURL());
        return new String(readBytes(conn.getInputStream()), StandardCharsets.UTF_8).trim();
    }

    private static byte[] readBytes(InputStream in) throws IOException {
        try (InputStream s = in) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] block = new byte[8192];
            int n;
            while ((n = s.read(block)) != -1) buf.write(block, 0, n);
            return buf.toByteArray();
        }
    }
}
