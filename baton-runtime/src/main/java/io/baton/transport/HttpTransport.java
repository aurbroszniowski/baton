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
package io.baton.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Blocking JDK HTTP transport. Long-poll callers supply their own read timeout. */
public final class HttpTransport {
    public static final int DEFAULT_TIMEOUT_MS = 10_000;

    private HttpTransport() {}

    public static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    public static HttpURLConnection open(String url, String method, int readTimeoutMs) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(DEFAULT_TIMEOUT_MS);
        conn.setReadTimeout(readTimeoutMs);
        return conn;
    }

    public static String post(String url) throws IOException {
        return post(url, new byte[0]);
    }

    public static String post(String url, byte[] body) throws IOException {
        return post(url, body, "application/octet-stream", DEFAULT_TIMEOUT_MS);
    }

    public static String post(String url, byte[] body, String contentType, int timeoutMs) throws IOException {
        return post(url, body, Map.of("Content-Type", contentType), timeoutMs, timeoutMs);
    }

    public static String post(String url, byte[] body, Map<String, String> headers,
                              int connectTimeoutMs, int readTimeoutMs) throws IOException {
        HttpURLConnection conn = open(url, "POST", readTimeoutMs);
        try {
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setDoOutput(true);
            headers.forEach(conn::setRequestProperty);
            try (OutputStream out = conn.getOutputStream()) {
                out.write(body);
            }
            return new String(readResponse(conn), StandardCharsets.UTF_8).trim();
        } finally {
            conn.disconnect();
        }
    }

    public static String get(String url) throws IOException {
        byte[] body = getBytes(url, DEFAULT_TIMEOUT_MS);
        return body == null ? "" : new String(body, StandardCharsets.UTF_8).trim();
    }

    public static byte[] getBytes(String url, int timeoutMs) throws IOException {
        HttpURLConnection conn = open(url, "GET", timeoutMs);
        try {
            if (conn.getResponseCode() == 204) return null;
            return readResponse(conn);
        } finally {
            conn.disconnect();
        }
    }

    public static void postFile(String url, Path file) throws IOException {
        try {
            int fragmentIndex = url.indexOf('#');
            String fragment = fragmentIndex < 0 ? "" : url.substring(fragmentIndex);
            String address = fragmentIndex < 0 ? url : url.substring(0, fragmentIndex);
            url = address + (address.contains("?") ? "&" : "?") + "mode="
                    + Integer.toOctalString(AgentFiles.permissions(Files.getPosixFilePermissions(file))) + fragment;
        } catch (UnsupportedOperationException ignored) {
            // Permission metadata is optional on non-POSIX filesystems.
        }
        HttpURLConnection conn = open(url, "POST", DEFAULT_TIMEOUT_MS);
        try {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            conn.setFixedLengthStreamingMode(Files.size(file));
            try (InputStream in = Files.newInputStream(file); OutputStream out = conn.getOutputStream()) {
                in.transferTo(out);
            }
            readResponse(conn);
        } finally {
            conn.disconnect();
        }
    }

    public static void download(String url, Path target) throws IOException {
        HttpURLConnection conn = open(url, "GET", DEFAULT_TIMEOUT_MS);
        try {
            requireSuccess(conn);
            if (target.getParent() != null) Files.createDirectories(target.getParent());
            try (InputStream in = conn.getInputStream(); OutputStream out = Files.newOutputStream(target)) {
                in.transferTo(out);
            }
        } finally {
            conn.disconnect();
        }
    }

    private static byte[] readResponse(HttpURLConnection conn) throws IOException {
        requireSuccess(conn);
        try (InputStream in = conn.getInputStream()) {
            return in.readAllBytes();
        }
    }

    private static void requireSuccess(HttpURLConnection conn) throws IOException {
        int code = conn.getResponseCode();
        if (code != 200) throw new IOException("HTTP " + code + " from " + conn.getURL());
    }
}
