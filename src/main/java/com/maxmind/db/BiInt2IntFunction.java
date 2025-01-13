package com.maxmind.db;

import java.util.function.ToIntBiFunction;

@FunctionalInterface
interface BiInt2IntFunction extends ToIntBiFunction<Integer, Integer> {

    @Override
    default int applyAsInt(Integer integer, Integer integer2) {
        return apply(integer, integer);
    }

    int apply(int integer0, int integer1);

}
