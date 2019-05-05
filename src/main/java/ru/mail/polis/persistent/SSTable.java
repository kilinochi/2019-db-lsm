package ru.mail.polis.persistent;


import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Iters;

import java.io.*;
import java.nio.ByteBuffer;
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


    public SSTable(final File file) throws IOException {
        final long fileSize = file.length();
//        final ByteBuffer mapped;
        final ByteBuffer mapped = ByteBuffer.allocate((int) fileSize * Integer.BYTES);
        try (
                FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fileSize <= Integer.MAX_VALUE;
//            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize).order(ByteOrder.BIG_ENDIAN);
            fc.read(mapped);
        }
        int limit = mapped.limit();
        // Rows
        final long rowsValue = mapped.getLong((int) (fileSize - Long.BYTES)); //fileSize - 8 byte
        assert rowsValue <= Integer.MAX_VALUE;
        this.rows = (int) rowsValue;

        // Offset
        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(limit - Long.BYTES * rows - Long.BYTES);
        offsetBuffer.limit(limit - Long.BYTES);
        this.offsets = offsetBuffer.slice().asLongBuffer();

        // Clusters
        final ByteBuffer clusterBuffer = mapped.duplicate();
        clusterBuffer.limit(offsetBuffer.position());
        this.clusters = clusterBuffer.slice();
    }


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

    static Iterator<Cluster> merge(List<SSTable> tableList) {
        List<Iterator<Cluster>> iteratorList = new ArrayList<>(tableList.size());
        tableList.forEach(table -> {
            iteratorList.add(table.iterator(ByteBuffer.allocate(0)));
        });
        Iterator<Cluster> iter = Iterators.mergeSorted(iteratorList, Cluster.COMPARATOR);
        iter = Iters.collapseEquals(iter);
        return iter;
    }

    static Iterator<Cluster> merge(SSTable ssTable, SSTable ssTable1) {
        List<SSTable> list = new ArrayList<>();
        list.add(ssTable);
        list.add(ssTable1);
        return merge(list);
    }

    private int position(final ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final ByteBuffer keyAt = keyAt(mid);
            final int cmp = from.compareTo(keyAt);
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

