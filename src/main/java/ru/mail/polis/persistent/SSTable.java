package ru.mail.polis.persistent;



import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTable {

    private final int rows;
    private final LongBuffer offsets;
    private final ByteBuffer clusters;


    public SSTable(final File file) throws IOException {
        final long fileSize = file.length();
        final ByteBuffer mapped;
        try (
                FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fileSize <= Integer.MAX_VALUE;
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize).order(ByteOrder.BIG_ENDIAN);
        }
        // Rows
        final long rowsValue = mapped.getLong((int) (fileSize - Long.BYTES)); //fileSize - 8 byte
        assert rowsValue <= Integer.MAX_VALUE;
        this.rows = (int) rowsValue;

        // Offset
        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(mapped.limit() - Long.BYTES * rows - Long.BYTES);
        offsetBuffer.limit(mapped.limit() - Long.BYTES);
        this.offsets = offsetBuffer.slice().asLongBuffer();

        // Clusters
        final ByteBuffer clusterBuffer = mapped.duplicate();
        clusterBuffer.limit(offsetBuffer.position());
        this.clusters = clusterBuffer.slice();
    }


    public Iterator<Cluster> iterator(ByteBuffer from) {
        return new Iterator<Cluster>() {

            int next = position(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cluster next() {
                assert hasNext();
                return clusterAt(next++);
            }
        };
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private ByteBuffer keyAt(int i) {
        assert 0 <= i && i < rows;
        final long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;
        final int keySize = clusters.getInt((int) offset);
        final ByteBuffer key = clusters.duplicate();
        key.position((int) (offset + Integer.BYTES));
        key.limit(key.position() + keySize);
        return key.slice();
    }
    private Cluster clusterAt(final int i) {
        assert 0 <= i && i < rows;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        //Key
        final int keySize = clusters.getInt((int) offset);
        offset += Integer.BYTES;
        final ByteBuffer key = clusters.duplicate();
        key.position((int) (offset + Integer.BYTES));
        key.limit(key.position() + keySize);
        offset += keySize;

        //Timestamp
        final long timeStamp = clusters.getLong((int) offset);
        offset += Long.BYTES;

        if (timeStamp < 0) {
            return new Cluster(key.slice(), new ClusterValue(null, -timeStamp, true));
        } else {
            final int valueSize = clusters.getInt((int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = clusters.duplicate();
            value.position((int) offset);
            value.limit(value.position() + valueSize);
            return new Cluster(key.slice(), new ClusterValue(value.slice(), timeStamp, false));
        }
    }
}

