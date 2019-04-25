package ru.mail.polis.persistent;


import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ClusterValue implements Comparable <ClusterValue> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean tombstone;

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public static ClusterValue of(final ByteBuffer data) {
        return new ClusterValue(data, System.currentTimeMillis(), false);
    }

    public static ClusterValue deadCluster() {
        return new ClusterValue(null, System.currentTimeMillis(), true);
    }

    private ClusterValue(ByteBuffer data, long timestamp, boolean isDead) {
        this.data = data;
        this.timestamp = timestamp;
        this.tombstone = isDead;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(@NotNull ClusterValue o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
