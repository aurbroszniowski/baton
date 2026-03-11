/*
 * Copyright Aurélien Broszniowski
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

import java.util.Map;

/**
 * A {@link ClassLoader} backed entirely by an in-memory map of {@code className -> bytecode}.
 *
 * <p>The agent instantiates one of these for each incoming {@link io.baton.ClassBundle} before
 * deserializing the lambda.  Classes not found here are delegated to the parent
 * (the agent's application classloader).
 */
public class BundleClassLoader extends ClassLoader {

    private final Map<String, byte[]> classes;

    public BundleClassLoader(Map<String, byte[]> classes, ClassLoader parent) {
        super(parent);
        this.classes = classes;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        byte[] bytes = classes.get(name);
        if (bytes != null) {
            return defineClass(name, bytes, 0, bytes.length);
        }
        throw new ClassNotFoundException(name);
    }
}
