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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

class LazyLinkedHashMapTest {
    @DisplayName("Test the size of the map - which should be equal to the number in the keySet.")
    @Test
    void testSize() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet,
                Integer::parseInt);
        Assertions.assertEquals(keySet.size(), map.size());
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test whether map is empty or not - which should be equal to the number in the keySet.")
    @Test
    void testIsEmpty() {
        LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet,
                Integer::parseInt);
        Assertions.assertFalse(map.isEmpty());
        Assertions.assertEquals(0, map.getLazyMapSize());

        keySet = new LinkedHashSet<>();
        map = new LazyLinkedHashMap<>(
                keySet,
                Integer::parseInt);
        Assertions.assertTrue(map.isEmpty());
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test whether map contains a key.")
    @Test
    void testContainsKey() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet,
                Integer::parseInt);
        Assertions.assertFalse(map.containsKey("3"));
        Assertions.assertTrue(map.containsKey("1"));
        Assertions.assertTrue(map.containsKey("2"));
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test the map contains a value - which is not supported.")
    @Test
    void testContainsValue() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> map.containsValue(1));
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test getting a specific value from the map - which will lazy load each value only once.")
    @Test
    void testGet() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(null, map.get("3"));
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertEquals(1, map.get("1"));
        Assertions.assertEquals(1, map.getLazyMapSize());
        Assertions.assertEquals(2, map.get("2"));
        Assertions.assertEquals(2, map.getLazyMapSize());

        Assertions.assertEquals(1, map.get("1"));
        Assertions.assertEquals(2, map.getLazyMapSize());
        Assertions.assertEquals(2, map.get("2"));
        Assertions.assertEquals(2, map.getLazyMapSize());
    }

    @DisplayName("Test putting a value in the map - which is not supported.")
    @Test
    void testPut() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> map.put("3", 3));
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test removing a value in the map - which is not supported.")
    @Test
    void testRemove() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> map.remove("2"));
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test putting all values in the map - which is not supported.")
    @Test
    void testPutAll() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> map.putAll(new LinkedHashMap<>()));
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test clearing the map - which is not supported.")
    @Test
    void testClear() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertEquals(2, map.size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> map.clear());
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertEquals(2, map.size());
    }

    @DisplayName("Test retrieving the keySet from map - does not retrieve any values.")
    @Test
    void testKeySet() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertArrayEquals(keySet.toArray(), map.keySet().toArray(new String[0]));
        Assertions.assertEquals(2, map.keySet().size());
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test getting all values in the map - which is not supported.")
    @Test
    void testValues() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> map.values());
        Assertions.assertEquals(0, map.getLazyMapSize());
    }

    @DisplayName("Test getting all entries (key/value) in the map - which is not supported.")
    @Test
    void testEntrySet() {
        final LinkedHashSet<String> keySet = new LinkedHashSet<>(Arrays.asList("1", "2"));
        final LazyLinkedHashMap<String, Integer> map = new LazyLinkedHashMap<>(
                keySet, Integer::parseInt);
        Assertions.assertEquals(0, map.getLazyMapSize());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> map.entrySet());
        Assertions.assertEquals(0, map.getLazyMapSize());
    }
}
