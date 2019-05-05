package ru.mail.polis.persistent;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cluster {

    public static final Comparator <Cluster> COMPARATOR = Comparator.comparing(Cluster::getKey).thenComparing(Cluster::getClusterValue);

    private final ByteBuffer key;
    private final ClusterValue clusterValue;

    public Cluster(ByteBuffer key, ClusterValue clusterValue) {
        this.key = key;
        this.clusterValue = clusterValue;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ClusterValue getClusterValue() {
        return clusterValue;
    }
}
