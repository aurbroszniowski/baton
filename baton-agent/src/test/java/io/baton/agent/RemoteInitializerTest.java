/*
 * Copyright Aurélien Broszniowski
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
package io.baton.agent;

import io.baton.NodeId;
import io.baton.RemoteInitializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AgentMain#runInitializers} and the {@link RemoteInitializer} SPI.
 */
class RemoteInitializerTest {

    private static final NodeId AGENT_ID = new NodeId("test-agent", "localhost", 0, 0);

    // runInitializers 

    @Test
    void runInitializers_callsInitializer(@TempDir Path workDir) {
        AtomicBoolean called = new AtomicBoolean(false);
        RemoteInitializer init = (agentId, dir) -> called.set(true);

        // Invoke the package-private method directly (same package as test)
        AgentMain.runInitializers(
                AGENT_ID, workDir, "run-id",
                List.of(init));

        assertTrue(called.get(), "Initializer must be called by runInitializers");
    }

    @Test
    void runInitializers_passesCorrectArguments(@TempDir Path workDir) {
        List<NodeId> receivedIds  = new ArrayList<>();
        List<Path>   receivedDirs = new ArrayList<>();

        RemoteInitializer init = (agentId, dir) -> {
            receivedIds.add(agentId);
            receivedDirs.add(dir);
        };

        AgentMain.runInitializers(AGENT_ID, workDir, "run-id", List.of(init));

        assertEquals(1, receivedIds.size());
        assertSame(AGENT_ID, receivedIds.get(0), "agentId passed to initializer must be the agent's NodeId");
        assertEquals(workDir, receivedDirs.get(0), "workDir passed to initializer must match");
    }

    @Test
    void runInitializers_multipleInitializers_allCalled(@TempDir Path workDir) {
        List<String> order = new ArrayList<>();

        AgentMain.runInitializers(AGENT_ID, workDir, "run-id", List.of(
                (id, dir) -> order.add("first"),
                (id, dir) -> order.add("second"),
                (id, dir) -> order.add("third")));

        assertEquals(List.of("first", "second", "third"), order,
                "All initializers must be called in discovery order");
    }

    @Test
    void runInitializers_exceptionInOne_othersStillRun(@TempDir Path workDir) {
        AtomicBoolean afterCalled = new AtomicBoolean(false);

        // Must NOT throw even if an initializer throws
        assertDoesNotThrow(() ->
                AgentMain.runInitializers(AGENT_ID, workDir, "run-id", List.of(
                        (id, dir) -> { throw new RuntimeException("intentional failure"); },
                        (id, dir) -> afterCalled.set(true))));

        assertTrue(afterCalled.get(), "Initializers after a failing one must still run");
    }

    @Test
    void runInitializers_emptyList_isNoOp(@TempDir Path workDir) {
        assertDoesNotThrow(() ->
                AgentMain.runInitializers(AGENT_ID, workDir, "run-id", List.of()));
    }
}
