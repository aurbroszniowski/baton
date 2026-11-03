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

import com.sun.net.httpserver.HttpServer;
import io.baton.NodeId;
import io.baton.transport.Serialization;
import org.junit.jupiter.api.Test;

import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class JobDispatcherTest {
    @Test
    void agentFailureAffectsOnlyItsJobsAndOtherResultsStillComplete() throws Exception {
        List<String> ids = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/job", exchange -> {
            try (ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody())) {
                ids.add(in.readUTF());
            }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        JobDispatcher dispatcher = new JobDispatcher();
        try {
            NodeId first = new NodeId("first", "localhost", server.getAddress().getPort(), 1);
            NodeId second = new NodeId("second", "localhost", server.getAddress().getPort(), 2);
            Future<Integer> failed = dispatcher.dispatchCallable(first, () -> 1);
            Future<Integer> successful = dispatcher.dispatchCallable(second, () -> 2);
            dispatcher.failAgent(first, new AgentDeadException("worker died"));
            assertInstanceOf(AgentDeadException.class,
                    assertThrows(ExecutionException.class, () -> failed.get(1, TimeUnit.SECONDS)).getCause());
            assertFalse(successful.isDone());
            dispatcher.completeJob(ids.get(1), Serialization.write(2), false);
            assertEquals(2, successful.get(1, TimeUnit.SECONDS));
            dispatcher.completeJob(ids.get(0), Serialization.write(1), false);
            assertTrue(failed.isDone(), "A late result cannot revive a failed job");
            dispatcher.failAll(new IllegalStateException("closed"));
            assertInstanceOf(IllegalStateException.class,
                    assertThrows(ExecutionException.class,
                            () -> dispatcher.dispatchCallable(second, () -> 3).get(1, TimeUnit.SECONDS)).getCause());
        } finally {
            dispatcher.failAll(new IllegalStateException("test ended"));
            server.stop(0);
        }
    }
}
