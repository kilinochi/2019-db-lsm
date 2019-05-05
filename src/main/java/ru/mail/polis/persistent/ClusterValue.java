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

    public ByteBuffer getValue() {
        return data.asReadOnlyBuffer();
    }

    public static ClusterValue of(final ByteBuffer data) {
        return new ClusterValue(data, System.currentTimeMillis(), false);
    }

    public static ClusterValue deadClusterValue() {
        return new ClusterValue(ByteBuffer.allocate(0), System.currentTimeMillis(), true);
    }

    public ClusterValue(ByteBuffer data, long timestamp, boolean isDead) {
        this.data = data;
        this.timestamp = timestamp;
        this.tombstone = isDead;
    }

    public boolean isTombstone() {
        return tombstone;
    }


    public int size(){
        if(tombstone) {
            return Long.BYTES;
        }
        else {
            return Long.BYTES + Integer.BYTES + data.remaining();
        }
    }



    @Override
    public int compareTo(@NotNull ClusterValue o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
