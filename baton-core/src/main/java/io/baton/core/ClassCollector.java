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

import io.baton.ClassBundle;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * An {@link ObjectOutputStream} subclass that intercepts class annotations during
 * serialization and collects the bytecode of every non-JDK class it encounters.
 *
 * <p>Usage:
 * <pre>{@code
 * ByteArrayOutputStream lambdaBuf = new ByteArrayOutputStream();
 * ClassCollector collector = new ClassCollector(lambdaBuf);
 * collector.writeObject(myLambda);
 * collector.flush();
 * ClassBundle bundle = new ClassBundle(collector.getClassBytes(), lambdaBuf.toByteArray());
 * }</pre>
 */
class ClassCollector extends ObjectOutputStream {

    private final Map<String, byte[]> classBytes = new LinkedHashMap<>();
    private final Set<String>         seen       = new LinkedHashSet<>();

    ClassCollector(OutputStream out) throws IOException {
        super(out);
        enableReplaceObject(true);
    }

    @Override
    protected void annotateClass(Class<?> cl) throws IOException {
        collectTransitively(cl);
    }

    void collectTransitively(Class<?> cl) {
        if (cl == null || cl.isArray() || isBootstrap(cl) || !seen.add(cl.getName())) return;

        addClassBytes(cl);
        collectTransitively(cl.getSuperclass());
        for (Class<?> iface : cl.getInterfaces()) collectTransitively(iface);
        Class<?> enclosing = cl.getEnclosingClass();
        if (enclosing != null) collectTransitively(enclosing);
        // Collect field types so that the remote JVM can link the class without CNFEs
        for (Field f : cl.getDeclaredFields()) {
            collectTransitively(f.getType());
        }
    }

    private void addClassBytes(Class<?> cl) {
        ClassLoader loader = cl.getClassLoader();
        if (loader == null) return;
        String path = cl.getName().replace('.', '/') + ".class";
        try (InputStream is = loader.getResourceAsStream(path)) {
            if (is != null) classBytes.put(cl.getName(), is.readAllBytes());
        } catch (IOException ignored) {
            // skip — dynamically generated or inaccessible class
        }
    }

    /**
     * Returns {@code true} for classes that are already on every JVM's classpath and
     * therefore do not need to be shipped.
     */
    static boolean isBootstrap(Class<?> cl) {
        if (cl.getClassLoader() == null) return true; // bootstrap classloader
        String n = cl.getName();
        return n.startsWith("java.")
                || n.startsWith("javax.")
                || n.startsWith("sun.")
                || n.startsWith("jdk.")
                || n.startsWith("com.sun.");
    }

    Map<String, byte[]> getClassBytes() {
        return Collections.unmodifiableMap(classBytes);
    }
}
