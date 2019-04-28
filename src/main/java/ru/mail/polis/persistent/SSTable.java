package ru.mail.polis.persistent;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

class SSTable {

    private final int rows;
    private final LongBuffer offsets;
    private final ByteBuffer cells;


    private SSTable(final File file) throws IOException {
        final long fileSize = file.length();
        final ByteBuffer mapped;
        try (
                FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fileSize <= Integer.MAX_VALUE;
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize).order(ByteOrder.BIG_ENDIAN);
        }
        // Rows
        final long rowsValue = mapped.getLong((int) (fileSize - Long.BYTES));
        assert rowsValue <= Integer.MAX_VALUE;
        this.rows = (int) rowsValue;

        // Offset
        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(mapped.limit() - Long.BYTES * rows - Long.BYTES);
        offsetBuffer.limit(mapped.limit() - Long.BYTES);
        this.offsets = offsetBuffer.slice().asLongBuffer();

        // Cells
        final ByteBuffer cellBuffer = mapped.duplicate();
        cellBuffer.limit(offsetBuffer.position());
        this.cells = cellBuffer.slice();
    }


    public Iterator<Cluster> iterator(ByteBuffer from) {
      return null;
    }
}

