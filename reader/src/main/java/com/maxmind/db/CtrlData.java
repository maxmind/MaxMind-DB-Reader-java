package com.maxmind.db;

final class CtrlData {
    private final Type type;
    private final int ctrlByte;
    private final long offset;
    private final long size;

    CtrlData(Type type, int ctrlByte, long offset, long size) {
        this.type = type;
        this.ctrlByte = ctrlByte;
        this.offset = offset;
        this.size = size;
    }

    public Type getType() {
        return this.type;
    }

    public int getCtrlByte() {
        return this.ctrlByte;
    }

    public long getOffset() {
        return this.offset;
    }

    public long getSize() {
        return this.size;
    }
}
