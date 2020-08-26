package com.maxmind.db;

class LRUElement {
    LRUElement older;
    LRUElement newer;

    CacheKey key;
    Object value;
}
