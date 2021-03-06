package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SSTable {

    private final int rows;
    private final LongBuffer offsets;
    private final ByteBuffer clusters;
    private final long generation;
    private final File table;

    /**
     * Write data as iterator in disk.
     *
     * @param clusters is the data, which we want to write
     * @param to       is the file in the directory in which we want
     *                 write data
     */
    static void writeToFile(@NotNull final Iterator<Cluster> clusters, @NotNull final File to)
            throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (clusters.hasNext()) {
                offsets.add(offset);

                final Cluster cluster = clusters.next();

                // Write Key
                final ByteBuffer key = cluster.getKey();
                final int keySize = cluster.getKey().remaining();
                fileChannel.write(BytesWrapper.fromInt(keySize));
                offset += Integer.BYTES; // 4 byte
                final ByteBuffer keyDuplicate = key.duplicate();
                fileChannel.write(keyDuplicate);
                offset += keySize;

                // Value
                final ClusterValue value = cluster.getClusterValue();

                // Write Timestamp
                if (value.isTombstone()) {
                    fileChannel.write(BytesWrapper.fromLong(-cluster.getClusterValue().getTimestamp()));
                } else {
                    fileChannel.write(BytesWrapper.fromLong(cluster.getClusterValue().getTimestamp()));
                }
                offset += Long.BYTES; // 8 byte

                // Write Value Size and Value

                if (!value.isTombstone()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fileChannel.write(BytesWrapper.fromInt(valueSize));
                    offset += Integer.BYTES; // 4 byte
                    fileChannel.write(valueData);
                    offset += valueSize;
                }
                //else - not write Value
            }
            // Write Offsets
            for (final Long anOffset : offsets) {
                fileChannel.write(BytesWrapper.fromLong(anOffset));
            }
            //Cells
            fileChannel.write(BytesWrapper.fromLong(offsets.size()));
        }
    }

    /**
     * File mapping from disk.
     *
     * @param file is the file from which we read data
     **/
    SSTable(@NotNull final File file, final long generation) throws IOException {
        this.generation = generation;
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
        this.table = file;
    }

    /**
     * Iterator of data from file.
     *
     * @param from is the key, which help to find necessary
     *             clusters of data
     **/
    public Iterator<Cluster> iterator(@NotNull final ByteBuffer from) {
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

    File getTable() {
        return table;
    }

    private int position(final @NotNull ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private ByteBuffer keyAt(final int i) {
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
        key.position((int) (offset));
        key.limit(key.position() + keySize);
        offset += keySize;

        //Timestamp
        final long timeStamp = clusters.getLong((int) offset);
        offset += Long.BYTES;

        if (timeStamp < 0) {
            return new Cluster(key.slice(), new ClusterValue(null, -timeStamp, true), generation);
        } else {
            final int valueSize = clusters.getInt((int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = clusters.duplicate();
            value.position((int) offset);
            value.limit(value.position() + valueSize)
                    .position((int) offset)
                    .limit((int) (offset + valueSize));
            return new Cluster(key.slice(), new ClusterValue(value.slice(), timeStamp, false), generation);
        }
    }
}