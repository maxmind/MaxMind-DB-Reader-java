package com.maxmind.db;

/**
 * This provides safe functionality for {@link ByteReader}s that only can
 * handle up to <code>Integer.MAX_VALUE</code> positions.
 */
public abstract class IntLimitedByteReader implements ByteReader {
    protected abstract ByteReader position(final int index);

    @Override
    public ByteReader position(long newPosition) {
        final boolean isLarge = newPosition > Integer.MAX_VALUE;
        if (isLarge || newPosition < Integer.MIN_VALUE) {
            throw PositionedByteReader.createPositionException(capacity(), newPosition, isLarge);
        }
        return position((int) newPosition);
    }

    protected abstract byte get(final int index);

    @Override
    public byte get(final long index) {
        /*
        bytes.get(int) will do a range check for [0,limit),
        but before that, we need to make sure that casting won't
        do anything unexpected, like making a negative long
        cast to a positive int.
         */
        if (index > Integer.MAX_VALUE || index < Integer.MIN_VALUE) {
            throw new IndexOutOfBoundsException();
        }
        return get((int) index);
    }
}
