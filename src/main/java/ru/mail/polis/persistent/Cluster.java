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
    private final long  generation;

    /**
     * Cluster is a memory cell in file.
     *
     * @param key is the key of this cell by which we can find this Cluster
     * @param clusterValue is the value in this cell
     * @param generation is the generation to which cluster is belong
     **/
    public Cluster(@NotNull final ByteBuffer key, @NotNull final ClusterValue clusterValue, final long generation) {
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
