package ru.mail.polis.persistent;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CustomDAO implements DAO {

    public static final String SUFFIX_DAT = ".dat";
    public static final String SUFFIX_TMP = ".tmp";

    private final File directory;
    private final long flushLimit;
    private MemTable memTable;
    private final Collection<Path> files;
    private int generation;


    public CustomDAO(final File directory, final long flushLimit) throws IOException {
       this.directory = directory;
       assert flushLimit >= 0L;
       this.flushLimit = flushLimit;
       memTable = new MemTable();
       files = new ArrayList<>();
       Files.walk(directory.toPath(), 1)
               .filter(path-> path.getFileName().toString().endsWith(SUFFIX_DAT))
               .forEach(files::add);
    }


    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List <Iterator <Cluster>> iters = new ArrayList<>();
        for(final Path path : this.files) {
            SSTable ssTable = new SSTable(path.toFile());
            iters.add(ssTable.iterator(from));
        }

        iters.add(memTable.iterator(from));
        final Iterator <Cluster> clusterIterator = Iters.collapseEquals(
                Iterators.mergeSorted(iters, Cluster.COMPARATOR),
                Cluster::getKey
        );
        final Iterator <Cluster> alive = Iterators.filter(
                clusterIterator, cluster-> {
                    assert cluster != null;
                    return !cluster.getClusterValue().isTombstone();
                }
        );
        return Iterators.transform(alive, cluster-> {
            assert cluster != null;
            return Record.of(cluster.getKey(), cluster.getClusterValue().getData());
        });
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if(memTable.size() > flushLimit) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        if(memTable.size() > flushLimit) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private void flush() throws IOException {
        final File tmp = new File(directory, generation + SUFFIX_TMP);
        SSTable.writeToFile(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(directory, generation + SUFFIX_DAT);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation = generation + 1;
        memTable = new MemTable();
    }
}
