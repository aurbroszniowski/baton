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
package io.baton.angela;

import io.baton.Fabric;
import io.baton.FabricFactory;
import org.terracotta.angela.agent.com.AgentID;
import org.terracotta.angela.agent.com.Executor;
import org.terracotta.angela.agent.com.grid.ClusterPrimitives;
import org.terracotta.angela.agent.com.grid.GridProvider;
import org.terracotta.angela.common.net.PortAllocator;

import java.util.Collection;
import java.util.UUID;

/**
 * Implements Angela's {@link GridProvider} SPI using Baton as the backend.
 *
 * <p>This is the only class in the adapter that implements an Angela interface.
 * The orchestrator HTTP server binds on the first port returned by
 * {@code portAllocator.reserve(1)}.
 */
public class BatonGridProvider implements GridProvider {

    private final Fabric fabric;

    public BatonGridProvider(UUID group, String instanceName,
                                 PortAllocator portAllocator,
                                 Collection<String> peers) {
        int orchestratorPort;
        try (PortAllocator.PortReservation reservation = portAllocator.reserve(1)) {
            orchestratorPort = reservation.next();
        }
        this.fabric = FabricFactory.create(orchestratorPort);
    }

    @Override
    public AgentID getAgentID() {
        return BatonExecutor.toAngela(fabric.getLocalNodeId());
    }

    @Override
    public Executor createExecutor(UUID group, AgentID agentID) {
        return new BatonExecutor(fabric, group);
    }

    @Override
    public ClusterPrimitives createClusterPrimitives() {
        return new BatonClusterPrimitives(fabric);
    }

    @Override
    public void close() {
        fabric.close();
    }
}
