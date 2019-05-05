package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ClusterValue implements Comparable<ClusterValue> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean tombstone;

    long getTimestamp() {
        return timestamp;
    }

    ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public static ClusterValue of(@NotNull final ByteBuffer data) {
        return new ClusterValue(data, System.currentTimeMillis(), false);
    }

    static ClusterValue deadCluster() {
        return new ClusterValue(null, System.currentTimeMillis(), true);
    }

    ClusterValue(final ByteBuffer data, final long timestamp, final boolean isDead) {
        this.data = data;
        this.timestamp = timestamp;
        this.tombstone = isDead;
    }

    boolean isTombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(@NotNull final ClusterValue o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
