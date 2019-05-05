package ru.mail.polis.persistent;

import java.nio.ByteBuffer;

public class BytesWrapper {
    private BytesWrapper() {
    }

    static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        result.putInt(value);
        result.rewind();
        return result;
    }

    static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        result.putLong(value);
        result.rewind();
        return result;
    }
}
