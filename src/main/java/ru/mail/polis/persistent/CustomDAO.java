package ru.mail.polis.persistent;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class CustomDAO implements DAO {

    private MemTable memTable;
    private final File base;

    public CustomDAO(@NotNull final File file) throws IOException {

       this.base = file;
       this.memTable = new MemTable(base);
       //Files.walk(base.toPath(), 1).filter(path -> path.getFileName().toString().endsWith("txt"));
    }


    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final Iterator <Cluster> clusters = memTable.iterator(from);
        final Iterator <Cluster> aliveClusters = Iterators.filter(clusters, cluster -> {
            assert cluster != null;
            return !cluster.getClusterValue().isTombstone();
        });
        return Iterators.transform(aliveClusters, cluster -> {
            assert cluster != null;
            return Record.of(cluster.getKey(), cluster.getClusterValue().getData());
        });
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {

    }
}
