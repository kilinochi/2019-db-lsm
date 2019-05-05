package ru.mail.polis.persistent;


import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;


public class MemTable {


    private NavigableMap<ByteBuffer, ClusterValue> storage;
    private long tableSize;

    public MemTable() {
        storage = new TreeMap<>();
    }

    final Iterator<Cluster> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(storage.tailMap(from)
                        .entrySet().iterator(),
                e -> {
                    assert e != null;
                    return new Cluster(e.getKey(), e.getValue());
                });
    }

    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final ClusterValue prev = storage.put(key, ClusterValue.of(value));
        if (prev == null) {
            tableSize = tableSize + key.remaining() + value.remaining();
        } else if (prev.isTombstone()) {
            tableSize = tableSize + value.remaining();
        } else {
            tableSize = tableSize + value.remaining() - prev.getData().remaining();
        }
    }

    public void remove(@NotNull final ByteBuffer key) {
        final ClusterValue prev = storage.put(key, ClusterValue.deadCluster());
        if (prev == null) {
            tableSize = tableSize + key.remaining();
        } else if (!prev.isTombstone()) {
            tableSize = tableSize - prev.getData().remaining();
        }
    }

    public long size() {
        return tableSize;
    }
}