package ru.mail.polis.persistent;


import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;


public class MemTable {

    private static AtomicLong counter = new AtomicLong(0);
    private NavigableMap<ByteBuffer, Cluster> storage;
    private long tableSize;
    private File directory;
    private long generation;
    private long flushLimit;



    public void flush(final Path path) throws IOException {
        try(FileChannel fc = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)){

            final ByteBuffer offsets = ByteBuffer.allocate(storage.size() * Integer.BYTES);
            int offset = 0;
            for(final Cluster cluster: storage.values()) {
                offsets.putInt(offset);
                final ByteBuffer clusterBuffer = ByteBuffer.allocate(cluster.size());
                final ByteBuffer key = cluster.getKey();
                final ClusterValue value = cluster.getClusterValue();
                clusterBuffer.putInt(key.remaining()).put(key); // key size and key
                if(value.isTombstone()) {
                    clusterBuffer.putLong(-value.getTimestamp());
                }
                else {
                    //timestamp + size value + value
                    clusterBuffer.putLong(value.getTimestamp())
                            .putInt(value.getValue().remaining())
                            .put(value.getValue());
                }
                clusterBuffer.rewind();
                fc.write(clusterBuffer);
                final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES)
                        .putInt(cluster.size())
                        .rewind();
                fc.write(size);
                storage = new TreeMap<>();
                tableSize = 0;
            }
        }
    }


    public long getTableSize() {
        return tableSize;
    }

    public MemTable(File directory, long flushLimit) {
        this.directory = directory;
        this.flushLimit = flushLimit;
        storage = new TreeMap<>();
    }

    @NotNull
    public Iterator<Cluster> iterator(@NotNull ByteBuffer from) throws IOException {
        return storage.tailMap(from).values().iterator();
    }

    public void upsert(@NotNull ByteBuffer key, ByteBuffer value) throws IOException {
        final Cluster cur = Cluster.of(key, value);
        final Cluster prev = storage.put(key, cur);
        if(prev == null) {
            tableSize = tableSize + cur.size();
        }
        else {
            tableSize = cur.getClusterValue().size() -
                    prev.getClusterValue().size();
        }
    }

    public long getGeneration() {
        return generation;
    }

    public void remove(@NotNull ByteBuffer key) throws IOException {
        final Cluster cur = Cluster.tombstoneCluster(key);
        final Cluster prev = storage.put(key, cur);
        if(prev == null) {
            tableSize = tableSize + cur.size();
        }
        else {
            tableSize = cur.getClusterValue().size() -
                    prev.getClusterValue().size();
        }
    }
}