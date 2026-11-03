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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HttpTransportTest {
    @Test
    void noContentIsEmptyTextAndAbsentBinaryData() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/empty", exchange -> {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();
        try {
            String url = "http://localhost:" + server.getAddress().getPort() + "/empty";
            assertEquals("", HttpTransport.get(url));
            assertNull(HttpTransport.getBytes(url, 1_000));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void fileUploadsAddPermissionMetadataToEitherUrlForm(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("payload.txt");
        Files.writeString(file, "payload");
        AtomicReference<String> query = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/upload", exchange -> {
            query.set(exchange.getRequestURI().getRawQuery());
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            String endpoint = "http://localhost:" + server.getAddress().getPort() + "/upload";
            for (String suffix : new String[]{"", "?path=some%20file", "?path=some%20file#fragment"}) {
                HttpTransport.postFile(endpoint + suffix, file);
                assertEquals("payload", body.get());
                if (Files.getFileStore(file).supportsFileAttributeView("posix")) {
                    String mode = "mode=" + Integer.toOctalString(AgentFiles.permissions(Files.getPosixFilePermissions(file)));
                    assertEquals(suffix.isEmpty() ? mode : "path=some%20file&" + mode, query.get());
                }
            }
        } finally {
            server.stop(0);
        }
    }
}
