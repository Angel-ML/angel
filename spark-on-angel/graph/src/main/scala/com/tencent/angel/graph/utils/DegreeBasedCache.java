/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.graph.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.PriorityQueue;

public class DegreeBasedCache<K, V> implements Serializable {
    private static final long serialVersionUID = -4565361098941909936L;

    private int capacity;

    private PriorityQueue<CacheEntry<K, V>> minHeap;
    private HashMap<K, CacheEntry<K, V>> hashMap;

    static public class CacheEntry<K, V> implements Comparable<CacheEntry<K, V>> {
        int degree;
        K key;
        V neighbors;

        public CacheEntry() {
            degree = 0;
            neighbors = null;
        }

        public CacheEntry(K key, V neighbors, int degree) {
            this.key = key;
            this.degree = degree;
            this.neighbors = neighbors;
        }

        @Override
        public int compareTo(CacheEntry<K, V> other) {
            return Integer.compare(degree, other.degree);
        }

        public V getNeighbors() {
            return neighbors;
        }

        public int getDegree() {
            return degree;
        }
    }

    private DegreeBasedCache(int capacity) {
        this.capacity = capacity;
        this.minHeap = new PriorityQueue<>(capacity);
        this.hashMap = new HashMap<>(capacity);
    }


    public static <K, V> DegreeBasedCache<K, V> newInstance(int capacity) {
        return new DegreeBasedCache<>(capacity);
    }

    public void put(K key, V value, int degree) {
        CacheEntry<K, V> entry = new CacheEntry<>(key, value, degree);

        minHeap.add(entry);
        hashMap.put(key, entry);

        // evict entry with the smallest degree
        if (minHeap.size() > capacity) {
            CacheEntry<K, V> victim = minHeap.poll();
            hashMap.remove(victim.key);
        }
    }

    public V get(K key) {
        CacheEntry<K, V> entry = hashMap.get(key);

        if (entry != null) {
            return entry.neighbors;
        }
        return null;
    }

    public boolean contains(K key) {
        return hashMap.containsKey(key);
    }

    public boolean empty() {
        return minHeap.isEmpty();
    }

}
