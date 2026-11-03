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
package io.baton;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Explicit remote application classpath, JVM properties, and working directory. */
public final class DeploymentOptions {
    private final List<Path> applicationJars;
    private final Map<String, String> jvmProperties;
    private final String workingDirectory;

    /**
     * Relative working directories are resolved against the remote user's home.
     * Property values may contain {@code ${workDir}}, expanded to that absolute directory.
     */
    public DeploymentOptions(List<Path> applicationJars, Map<String, String> jvmProperties,
                             String workingDirectory) {
        this.applicationJars = List.copyOf(applicationJars);
        this.jvmProperties = Map.copyOf(jvmProperties);
        this.workingDirectory = Objects.requireNonNull(workingDirectory, "workingDirectory");
        if (workingDirectory.isEmpty()) throw new IllegalArgumentException("Working directory cannot be empty");
    }

    public List<Path> getApplicationJars() { return applicationJars; }
    public Map<String, String> getJvmProperties() { return jvmProperties; }
    public String getWorkingDirectory() { return workingDirectory; }
}
