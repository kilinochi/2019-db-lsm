package ru.mail.polis.persistent;


import com.google.common.primitives.Bytes;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class SSTable {

    private static SSTable instance;
    private final File file;

    private SSTable(File file) {
        this.file = file;
    }

    public static SSTable entity (File file) {
        SSTable localInstance = instance;
        if (localInstance == null) {
            synchronized (MemTable.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = localInstance = new SSTable(file);
                }
            }
        }
        return localInstance;
    }

    public void upsert(Iterator <Cluster> clusters) throws IOException {
        try(FileChannel fileChannel = FileChannel.open(
                file.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)){
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (clusters.hasNext()) {
                offsets.add(offset);

                final Cluster cluster = clusters.next();

                // Key
                final ByteBuffer key = cluster.getKey();
                final int keySize = cluster.getKey().remaining();
                fileChannel.write(BytesWrapper.fromInt(keySize));
                offset += Integer.BYTES;
                fileChannel.write(key);
                offset += keySize;

                // Value
                final ClusterValue value = cluster.getClusterValue();

                // Timestamp
                if (value.isTombstone()) {
                    fileChannel.write(BytesWrapper.fromLong(-cluster.getClusterValue().getTimestamp()));
                } else {
                    fileChannel.write(BytesWrapper.fromLong(cluster.getClusterValue().getTimestamp()));
                }
                offset += Long.BYTES;

                // Value

                if (!value.isTombstone()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fileChannel.write(BytesWrapper.fromInt(valueSize));
                    offset += Integer.BYTES;
                    fileChannel.write(valueData);
                    offset += valueSize;
                }
            }
            // Offsets
            for (final Long anOffset : offsets) {
                fileChannel.write(BytesWrapper.fromLong(anOffset));
            }
            //Cells
            fileChannel.write(BytesWrapper.fromLong(offsets.size()));
        }
    }
}

