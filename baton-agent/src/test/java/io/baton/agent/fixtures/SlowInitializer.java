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
package io.baton.agent.fixtures;

import io.baton.NodeId;
import io.baton.RemoteCallable;
import io.baton.RemoteInitializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/** Packaged separately so the real agent discovers this initializer through ServiceLoader. */
public final class SlowInitializer implements RemoteInitializer {
    private static volatile boolean initialized;

    @Override
    public void initialize(NodeId node, Path directory) {
        try {
            Path port = directory.resolve("initializer.port");
            Files.writeString(port, Integer.toString(node.getPort()));
            Files.move(port, directory.resolve("initializer.entered"));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!Files.exists(directory.resolve("initializer.release"))) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("Initializer release timed out");
                Thread.sleep(10);
            }
            initialized = true;
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public static RemoteCallable<Boolean> probe() { return () -> initialized; }
}
