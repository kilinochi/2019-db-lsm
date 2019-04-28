package ru.mail.polis.persistent;


import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static ru.mail.polis.DAOFactory.MAX_HEAP;


public class MemTable {

    private static AtomicLong counter = new AtomicLong(0);
    private NavigableMap<ByteBuffer, ClusterValue> storage;
    private static MemTable instance;
    private long tableSize;
    private File directory;

    public static MemTable entity (File file) {
        MemTable localInstance = instance;
        if (localInstance == null) {
            synchronized (MemTable.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = localInstance = new MemTable(file);
                }
            }
        }
        return localInstance;
    }

    @NotNull
    public Iterator<Cluster> iterator(@NotNull ByteBuffer from) throws IOException {
        return Iterators.transform(
                storage.tailMap(from).entrySet().iterator(), e -> {
                    assert e != null;
                    return new Cluster(e.getKey(), e.getValue());
                }
        );
    }

    private MemTable(File file) {
        this.directory = file;
        storage = new ConcurrentSkipListMap<>();
    }

    public void upsert(@NotNull ByteBuffer key, ByteBuffer value) throws IOException {
        final ClusterValue prev = storage.put(key, ClusterValue.of(value));
        if(prev == null) {
            tableSize = tableSize + key.remaining() + value.remaining();
        }
        else if(prev.isTombstone()) {
            tableSize = tableSize + value.remaining();
        }
        else {
            tableSize = tableSize + value.remaining() - prev.getData().remaining();
        }
        if (tableSize >= MAX_HEAP / 3) {
            long generation = counter.incrementAndGet();
            WriteToFileHelper.writeToFile(this.iterator(ByteBuffer.allocate(0)), directory, generation);
            storage = null;
            storage = new ConcurrentSkipListMap<>();
            tableSize = 0;
        }
    }

    public void remove(@NotNull ByteBuffer key) throws IOException {
        final ClusterValue prev = storage.put(key, ClusterValue.deadCluster());
        if (prev == null) {
            tableSize = tableSize + key.remaining();
        } else if (prev.isTombstone()) {

        } else {
            tableSize -= prev.getData().remaining();
        }
    }
}