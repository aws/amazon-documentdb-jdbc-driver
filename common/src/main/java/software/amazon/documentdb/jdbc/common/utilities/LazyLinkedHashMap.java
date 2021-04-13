/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc.common.utilities;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Implements a lazy {@link LinkedHashMap} where the keySet is set in the constructor, but
 * the get() is lazy loaded.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public class LazyLinkedHashMap<K,V> implements Map<K,V> {
    private final ImmutableSet<K> keySet;
    private final Map<K,V> map;
    private final Function<K,V> factory;

    /**
     * Constructs a new {@link LazyLinkedHashMap} with a given keySet and a factory function.
     * This map is a read-only map and does not support adding entries or updating existing entries.
     * The keySet should provide the fixed set of keys for this map.
     * The factory function will be invoked when the client calls the {@code get(key)} method.
     *
     * @param keySet the keySet to use.
     * @param factory the factory method to retrieve the instance at the map.
     */
    public LazyLinkedHashMap(final LinkedHashSet<K> keySet, final Function<K,V> factory) {
        this.keySet = ImmutableSet.copyOf(keySet);
        this.factory = factory;
        this.map = new LinkedHashMap<>();
    }

    @Override
    public int size() {
        return keySet.size();
    }

    @Override
    public boolean isEmpty() {
        return keySet.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(final Object key) {
        return keySet.contains((K) key);
    }

    @Override
    public boolean containsValue(final Object value) {
        // This would defeat purpose of "lazy load".
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final Object key) {
        if (!keySet.contains((K) key)) {
            return null;
        }
        if (!map.containsKey((K) key)) {
            map.put((K) key, factory.apply((K) key));
        }
        return map.get(key);
    }

    @Override
    public V put(final K key, final V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        return keySet;
    }

    @Override
    public Collection<V> values() {
        // This would defeat purpose of "lazy load".
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        // This would defeat purpose of "lazy load".
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    int getLazyMapSize() {
        return map.size();
    }
}
