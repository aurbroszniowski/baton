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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class AgentFilesTest {
    @Test
    void aliasesStayWithinTheirConfiguredRoot(@TempDir Path directory) {
        String property = "baton.path.test-data";
        String previous = System.getProperty(property);
        System.setProperty(property, directory.toString());
        try {
            assertEquals(directory, AgentFiles.resolve("test-data://"));
            assertEquals(directory.resolve("result.txt"), AgentFiles.resolve("test-data://sub/../result.txt"));
            assertThrows(IllegalArgumentException.class, () -> AgentFiles.resolve("test-data:///etc/passwd"));
            assertThrows(IllegalArgumentException.class, () -> AgentFiles.resolve("test-data://../outside.txt"));
            assertThrows(IllegalArgumentException.class, () -> AgentFiles.resolve("test-data://sub/../../outside.txt"));
            assertEquals(directory.resolve("absolute.txt"), AgentFiles.resolve(directory.resolve("absolute.txt").toString()));
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }
}
