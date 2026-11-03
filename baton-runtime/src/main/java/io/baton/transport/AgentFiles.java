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
package io.baton.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;

/** Local file operations and path expansion used by both agent entry points. */
public final class AgentFiles {
    private AgentFiles() {}

    public static Path resolve(String value) {
        int separator = value.indexOf("://");
        if (separator > 0) {
            String alias = value.substring(0, separator);
            String root = System.getProperty("baton.path." + alias);
            // Preserve the existing Angela adapter's path convention.
            if (root == null && alias.equals("angela-root")) {
                root = System.getProperty("angela.rootDir", System.getProperty("user.home") + "/.angela");
            }
            if (root == null) throw new IllegalArgumentException("Unknown path alias: " + alias);
            Path base = Path.of(root).toAbsolutePath().normalize();
            Path suffix = Path.of(value.substring(separator + 3));
            Path resolved = base.resolve(suffix).normalize();
            if (suffix.isAbsolute() || !resolved.startsWith(base)) {
                throw new IllegalArgumentException("Path escapes alias root: " + alias);
            }
            return resolved;
        }
        if (value.equals("~")) return Path.of(System.getProperty("user.home"));
        if (value.startsWith("~/")) return Path.of(System.getProperty("user.home")).resolve(value.substring(2));
        return Path.of(value);
    }

    public static void copy(Path source, Path target) throws IOException {
        if (target.getParent() != null) Files.createDirectories(target.getParent());
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    public static void upload(Path source, String target) throws IOException {
        if (Files.isDirectory(source)) {
            try (Stream<Path> files = Files.walk(source)) {
                for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                    copy(file, resolve(target).resolve(source.relativize(file)));
                }
            }
        } else {
            copy(source, resolve(target));
        }
    }

    public static int permissions(Set<PosixFilePermission> permissions) {
        int mode = 0;
        for (PosixFilePermission permission : permissions) mode |= 1 << (8 - permission.ordinal());
        return mode;
    }

    public static Set<PosixFilePermission> permissions(int mode) {
        Set<PosixFilePermission> permissions = EnumSet.noneOf(PosixFilePermission.class);
        for (PosixFilePermission permission : PosixFilePermission.values()) {
            if ((mode & (1 << (8 - permission.ordinal()))) != 0) permissions.add(permission);
        }
        return permissions;
    }
}
