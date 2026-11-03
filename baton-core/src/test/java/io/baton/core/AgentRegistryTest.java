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

import io.baton.NodeId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class AgentRegistryTest {
    @Test
    void registrationReturnsActualIdentityAndTimesOutForMissingAgents() throws Exception {
        AgentRegistry registry = new AgentRegistry();
        try {
            NodeId real = new NodeId("worker", "localhost", 8700, 1234);
            CompletableFuture<NodeId> registered = CompletableFuture.supplyAsync(() -> {
                try { return registry.awaitRegistration("localhost", 8700, 1_000); }
                catch (Exception e) { throw new RuntimeException(e); }
            });
            registry.register(real);
            assertEquals(real, registered.get(2, TimeUnit.SECONDS));
            long initialGeneration = registry.registrationGeneration();
            assertThrows(TimeoutException.class, () -> registry.awaitRegistration("localhost", 8700, 20, initialGeneration));
            registry.register(real);
            assertEquals(real, registry.awaitRegistration("localhost", 8700, 100, initialGeneration),
                    "Re-registering the same identity must advance the registration generation");
            long previous = registry.registrationGeneration();
            NodeId replacement = new NodeId("worker", "localhost", 8700, 5678);
            registry.register(replacement);
            assertEquals(replacement, registry.awaitRegistration("localhost", 8700, 100, previous));
            assertEquals(List.of(replacement), new ArrayList<>(registry.getConnectedNodes()));
            assertThrows(TimeoutException.class, () -> registry.awaitRegistration("missing", 8700, 20));
        } finally {
            registry.shutdown();
        }
    }

    @Test
    void expiredAgentsLeaveConnectedNodesAndRejectNewJobs() throws Exception {
        AgentRegistry registry = new AgentRegistry();
        JobDispatcher dispatcher = new JobDispatcher(registry::isAlive);
        List<NodeId> dead = new ArrayList<>();
        registry.setDeathListener(node -> {
            dead.add(node);
            dispatcher.failAgent(node, new AgentDeadException("expired"));
        });
        try {
            NodeId node = new NodeId("worker", "localhost", 8700, 1234);
            registry.register(node);
            registry.checkHeartbeats(System.currentTimeMillis() + AgentRegistry.GRACE_PERIOD_MS + 1);
            registry.checkHeartbeats(System.currentTimeMillis() + AgentRegistry.GRACE_PERIOD_MS + 2);
            assertEquals(List.of(node), dead);
            assertFalse(registry.isAlive(node));
            assertTrue(registry.getConnectedNodes().isEmpty());
            assertFalse(registry.heartbeat(node), "A late heartbeat cannot revive an expired identity");
            java.util.concurrent.ExecutionException failure = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> dispatcher.dispatchCallable(node, () -> 42).get(1, TimeUnit.SECONDS));
            assertInstanceOf(AgentDeadException.class, failure.getCause());
        } finally {
            registry.shutdown();
            dispatcher.failAll(new IllegalStateException("test ended"));
        }
    }
}
