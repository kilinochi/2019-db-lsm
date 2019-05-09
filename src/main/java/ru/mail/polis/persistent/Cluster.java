package ru.mail.polis.persistent;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cluster {

    static final Comparator<Cluster> COMPARATOR = Comparator
            .comparing(Cluster::getKey)
            .thenComparing(Cluster::getClusterValue)
            .thenComparing(Cluster::getGeneration, Comparator.reverseOrder());

    private final ByteBuffer key;
    private final ClusterValue clusterValue;
    private long  generation;

    public Cluster(@NotNull final ByteBuffer key, @NotNull final ClusterValue clusterValue, long generation) {
        this.key = key;
        this.clusterValue = clusterValue;
        this.generation = generation;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public long getGeneration() {
        return generation;
    }

    ClusterValue getClusterValue() {
        return clusterValue;
    }
}
