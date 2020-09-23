package com.maxmind.db;

final class CtrlData {
    private final Type type;
    private final int ctrlByte;
    private final int offset;
    private final int size;

    CtrlData(Type type, int ctrlByte, int offset, int size) {
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

    public int getOffset() {
        return this.offset;
    }

    public int getSize() {
        return this.size;
    }
}
