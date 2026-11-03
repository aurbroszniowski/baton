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
package io.baton.deployer;

import io.baton.DeploymentOptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AgentDeployerTest {
    @Test
    void explicitDeploymentUsesOnlySuppliedJarsPropertiesAndDirectory() {
        DeploymentOptions options = new DeploymentOptions(List.of(Path.of("application.jar")),
                Map.of("application.message", "a value with 'quotes'", "baton.path.data", "${workDir}/data"),
                "custom directory");
        String directory = AgentDeployer.resolveWorkingDirectory("/home/alice", options.getWorkingDirectory());
        String command = AgentDeployer.startCommand("worker", "http://host:9400", 8700, directory, options);
        assertTrue(command.contains("/home/alice/custom directory/lib/application.jar"));
        assertTrue(command.contains("'-Dapplication.message=a value with '\"'\"'quotes'\"'\"''"));
        assertTrue(command.contains("'-Dbaton.path.data=/home/alice/custom directory/data'"));
        assertFalse(command.contains("angela"));
        assertEquals("/absolute/path", AgentDeployer.resolveWorkingDirectory("/home/alice", "/absolute/path"));
        assertEquals("/home/alice/baton", AgentDeployer.resolveWorkingDirectory("/home/alice", "~/baton"));
    }
}
