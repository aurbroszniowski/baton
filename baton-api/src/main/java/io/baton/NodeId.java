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

import java.io.Serializable;
import java.util.Objects;

/**
 * Identifies a remote node (agent JVM) in the fabric.
 * Serializable so it can be captured inside remote lambdas.
 */
public final class NodeId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String hostname;
    private final int port;   // agent's HTTP port
    private final int pid;

    public NodeId(String name, String hostname, int port, int pid) {
        this.name = Objects.requireNonNull(name, "name");
        this.hostname = Objects.requireNonNull(hostname, "hostname");
        this.port = port;
        this.pid = pid;
    }

    public String getName()     { return name; }
    public String getHostname() { return hostname; }
    public int    getPort()     { return port; }
    public int    getPid()      { return pid; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeId)) return false;
        NodeId that = (NodeId) o;
        return port == that.port && pid == that.pid
                && name.equals(that.name) && hostname.equals(that.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, hostname, port, pid);
    }

    /**
     * Returns the canonical string representation used for HTTP headers and
     * internal correlation: {@code name@hostname:port#pid}.
     */
    @Override
    public String toString() {
        return name + "@" + hostname + ":" + port + "#" + pid;
    }

    /**
     * Returns a compact, human-readable log prefix: {@code baton-name@hostname#pid}.
     * The port is omitted because it is already encoded in the name (e.g. {@code agent-8700}).
     */
    public String toLogTag() {
        return "baton-" + name + "@" + hostname + "#" + pid;
    }
}
