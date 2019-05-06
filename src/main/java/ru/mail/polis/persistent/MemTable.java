package ru.mail.polis.persistent;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;


public class MemTable {


    private final NavigableMap<ByteBuffer, ClusterValue> storage;
    private long tableSize;

    public MemTable() {
        storage = new TreeMap<>();
    }

    public final Iterator<Cluster> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(storage.tailMap(from)
                        .entrySet().iterator(),
                e -> {
                    assert e != null;
                    return new Cluster(e.getKey(), e.getValue());
                });
    }

    /**
    * Insert new Value to storage.
    * @param key is the label which we can find data
    * @param value is the data
    **/

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

    /**
     *Delete Value from storage by key.
     * @param key is the label which we can find data
     *            and delete data from storage
     */

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