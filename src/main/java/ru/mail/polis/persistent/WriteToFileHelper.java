package ru.mail.polis.persistent;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class WriteToFileHelper {

    private static final String FILE_NAME = "SSTable_";
    private static final String SUFFIX = ".txt";

    public static void writeToFile(Iterator<Cluster> clusters, File directory, long generation) throws IOException {
        try(FileChannel fileChannel = FileChannel.open(
                Path.of(directory.getAbsolutePath(), FILE_NAME+generation+SUFFIX), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
        {
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
                ByteBuffer keyDuplicate = key.duplicate();
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
                    final ByteBuffer valueData = value.getValue();
                    final int valueSize = value.getValue().remaining();
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
}
