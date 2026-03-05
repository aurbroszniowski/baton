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

/**
 * SSH connection parameters used by {@link Fabric#deployAndConnect}.
 */
public final class SshConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String username;
    private final String identityFile; // path to private key, e.g. "~/.ssh/id_rsa"
    private final int port;

    private SshConfig(String username, String identityFile, int port) {
        this.username = username;
        this.identityFile = identityFile;
        this.port = port;
    }

    public static SshConfig of(String username, String identityFile) {
        return new SshConfig(username, identityFile, 22);
    }

    public static SshConfig of(String username, String identityFile, int sshPort) {
        return new SshConfig(username, identityFile, sshPort);
    }

    public String getUsername()     { return username; }
    public String getIdentityFile() { return identityFile; }
    public int    getPort()         { return port; }
}
