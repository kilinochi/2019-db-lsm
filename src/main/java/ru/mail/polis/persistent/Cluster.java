package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cluster {

    static final Comparator<Cluster> COMPARATOR = Comparator
            .comparing(Cluster::getKey)
            .thenComparing(Cluster::getClusterValue);

    private final ByteBuffer key;
    private final ClusterValue clusterValue;

    Cluster(@NotNull final ByteBuffer key, @NotNull final ClusterValue clusterValue) {
        this.key = key;
        this.clusterValue = clusterValue;
    }

    public ByteBuffer getKey() {
        return key;
    }

    ClusterValue getClusterValue() {
        return clusterValue;
    }
}
