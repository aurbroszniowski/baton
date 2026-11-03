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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Compatibility defaults for callers using the original deployment API. */
final class LegacyDeploymentOptions {
    private LegacyDeploymentOptions() {}

    static DeploymentOptions fromCurrentProcess() {
        List<Path> jars = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path", "").split(File.pathSeparator)) {
            Path path = Path.of(entry);
            if (entry.endsWith(".jar") && Files.isRegularFile(path)) jars.add(path);
        }
        Map<String, String> properties = new HashMap<>();
        for (String key : System.getProperties().stringPropertyNames()) {
            if (key.startsWith("angela.")) properties.put(key, System.getProperty(key));
        }
        properties.put("angela.rootDir", "${workDir}/angela-root");
        properties.put("baton.path.angela-root", "${workDir}/angela-root");
        return new DeploymentOptions(jars, properties, "baton");
    }
}
