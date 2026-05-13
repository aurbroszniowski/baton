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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;

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

    /** POST streaming a file without buffering it in memory. Also transmits POSIX permissions. */
    static void postFile(String url, Path file) throws IOException {
        // Append POSIX mode so the remote can restore execute bits (e.g. on .sh scripts)
        try {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(file);
            url = url + "&mode=" + Integer.toOctalString(posixToInt(perms));
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX filesystem — skip
        }
        HttpURLConnection conn = open(url, "POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setFixedLengthStreamingMode(Files.size(file));
        try (InputStream in = Files.newInputStream(file);
             OutputStream out = conn.getOutputStream()) {
            pipe(in, out);
        }
        int code = conn.getResponseCode();
        if (code != 200) throw new IOException("HTTP " + code + " from " + url);
    }

    static int posixToInt(Set<PosixFilePermission> perms) {
        int mode = 0;
        if (perms.contains(PosixFilePermission.OWNER_READ))     mode |= 0400;
        if (perms.contains(PosixFilePermission.OWNER_WRITE))    mode |= 0200;
        if (perms.contains(PosixFilePermission.OWNER_EXECUTE))  mode |= 0100;
        if (perms.contains(PosixFilePermission.GROUP_READ))     mode |= 0040;
        if (perms.contains(PosixFilePermission.GROUP_WRITE))    mode |= 0020;
        if (perms.contains(PosixFilePermission.GROUP_EXECUTE))  mode |= 0010;
        if (perms.contains(PosixFilePermission.OTHERS_READ))    mode |= 0004;
        if (perms.contains(PosixFilePermission.OTHERS_WRITE))   mode |= 0002;
        if (perms.contains(PosixFilePermission.OTHERS_EXECUTE)) mode |= 0001;
        return mode;
    }

    static Set<PosixFilePermission> intToPosix(int mode) {
        Set<PosixFilePermission> perms = EnumSet.noneOf(PosixFilePermission.class);
        if ((mode & 0400) != 0) perms.add(PosixFilePermission.OWNER_READ);
        if ((mode & 0200) != 0) perms.add(PosixFilePermission.OWNER_WRITE);
        if ((mode & 0100) != 0) perms.add(PosixFilePermission.OWNER_EXECUTE);
        if ((mode & 0040) != 0) perms.add(PosixFilePermission.GROUP_READ);
        if ((mode & 0020) != 0) perms.add(PosixFilePermission.GROUP_WRITE);
        if ((mode & 0010) != 0) perms.add(PosixFilePermission.GROUP_EXECUTE);
        if ((mode & 0004) != 0) perms.add(PosixFilePermission.OTHERS_READ);
        if ((mode & 0002) != 0) perms.add(PosixFilePermission.OTHERS_WRITE);
        if ((mode & 0001) != 0) perms.add(PosixFilePermission.OTHERS_EXECUTE);
        return perms;
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

    // Helpers 

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

    static void pipe(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[64 * 1024];
        int n;
        while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
    }
}
