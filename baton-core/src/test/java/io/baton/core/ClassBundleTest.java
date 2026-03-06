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
import io.baton.RemoteCallable;
import io.baton.RemoteRunnable;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 — tests the {@link ClassCollector} / {@link ClassBundle} / {@link BundleClassLoader}
 * serialization pipeline end-to-end, entirely in-process.
 */
class ClassBundleTest {

    // ── ClassCollector ─────────────────────────────────────────────────────────

    @Test
    void classCollector_capturesLambdaSyntheticClass() throws IOException {
        RemoteRunnable job = () -> { /* no-op */ };

        ClassBundle bundle = buildBundle(job);

        assertFalse(bundle.classes.isEmpty(),
                "ClassCollector should have captured at least the lambda's synthetic class");
        bundle.classes.values().forEach(bytes ->
                assertTrue(bytes.length > 0, "Captured class must have non-empty bytecode"));
    }

    @Test
    void classCollector_doesNotCaptureJdkClasses() throws IOException {
        RemoteRunnable job = () -> { String s = String.valueOf(42); };

        ClassBundle bundle = buildBundle(job);

        bundle.classes.keySet().forEach(name ->
                assertFalse(name.startsWith("java."),
                        "JDK class should not be bundled: " + name));
    }

    @Test
    void classCollector_lambdaBytesAreNonEmpty() throws IOException {
        RemoteRunnable job = () -> { /* no-op */ };

        ClassBundle bundle = buildBundle(job);

        assertNotNull(bundle.lambda);
        assertTrue(bundle.lambda.length > 0);
    }

    // ── BundleClassLoader ─────────────────────────────────────────────────────

    @Test
    void bundleClassLoader_definesClassFromBytecode() throws Exception {
        RemoteRunnable job = () -> { /* no-op */ };
        ClassBundle bundle = buildBundle(job);

        BundleClassLoader loader = new BundleClassLoader(bundle.classes, getClass().getClassLoader());

        for (String name : bundle.classes.keySet()) {
            Class<?> loaded = loader.loadClass(name);
            assertNotNull(loaded);
            assertEquals(name, loaded.getName());
        }
    }

    // ── Full round-trip: ClassCollector → ClassBundle → BundleClassLoader → execute ──

    @Test
    void roundTrip_remoteRunnable_executes() throws Exception {
        AtomicBoolean ran = new AtomicBoolean(false);
        RemoteRunnable job = () -> ran.set(true);

        RemoteRunnable deserialized = (RemoteRunnable) roundTrip(buildBundle(job));
        deserialized.run();

        // Note: in Phase 1 the deserialized lambda has its own AtomicBoolean copy,
        // so we verify execution succeeded without exception — state sharing is a Phase 2 concern.
        assertNotNull(deserialized);
    }

    @Test
    void roundTrip_remoteCallable_returnsValue() throws Exception {
        RemoteCallable<Integer> job = () -> 42;

        @SuppressWarnings("unchecked")
        RemoteCallable<Integer> deserialized = (RemoteCallable<Integer>) roundTrip(buildBundle(job));

        assertEquals(42, deserialized.call());
    }

    @Test
    void roundTrip_remoteCallable_returnsString() throws Exception {
        RemoteCallable<String> job = () -> "hello from bundle";

        @SuppressWarnings("unchecked")
        RemoteCallable<String> deserialized = (RemoteCallable<String>) roundTrip(buildBundle(job));

        assertEquals("hello from bundle", deserialized.call());
    }

    @Test
    void roundTrip_lambdaCapturingPrimitive() throws Exception {
        int value = 99; // effectively final primitive — serialized by value, safe across JVMs
        RemoteCallable<Integer> job = () -> value;

        @SuppressWarnings("unchecked")
        RemoteCallable<Integer> deserialized = (RemoteCallable<Integer>) roundTrip(buildBundle(job));

        assertEquals(99, deserialized.call());
    }

    @Test
    void roundTrip_lambdaCapturingString() throws Exception {
        String msg = "cross-jvm payload";
        RemoteCallable<String> job = () -> msg.toUpperCase();

        @SuppressWarnings("unchecked")
        RemoteCallable<String> deserialized = (RemoteCallable<String>) roundTrip(buildBundle(job));

        assertEquals("CROSS-JVM PAYLOAD", deserialized.call());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ClassBundle buildBundle(Object job) throws IOException {
        ByteArrayOutputStream lambdaBuf = new ByteArrayOutputStream();
        ClassCollector collector = new ClassCollector(lambdaBuf);
        collector.writeObject(job);
        collector.flush();
        return new ClassBundle(collector.getClassBytes(), lambdaBuf.toByteArray());
    }

    private Object roundTrip(ClassBundle bundle) throws Exception {
        BundleClassLoader loader = new BundleClassLoader(bundle.classes, getClass().getClassLoader());
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bundle.lambda)) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
                try {
                    return loader.loadClass(desc.getName());
                } catch (ClassNotFoundException e) {
                    return super.resolveClass(desc);
                }
            }
        };
        return ois.readObject();
    }
}
