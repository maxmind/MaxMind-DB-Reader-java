package com.maxmind.db;

class LRUElement {
    LRUElement older;
    LRUElement newer;

    CacheKey key;
    CacheValue value;
}
