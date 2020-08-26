package com.maxmind.db;

import java.util.LinkedHashMap;

class LinkedHashMapLRU extends LinkedHashMap<CacheKey, Object> {
    private int capacity;

    public LinkedHashMapLRU(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<CacheKey, Object> eldest) {
        return size() >= capacity;
    }
}
