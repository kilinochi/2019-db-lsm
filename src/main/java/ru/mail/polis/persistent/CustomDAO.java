package ru.mail.polis.persistent;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomDAO implements DAO {

    private MemTable memTable;
    private final File baseDirectory;
    private List <SSTable> ssTables;

    public CustomDAO(@NotNull final File baseDirectory, long flushLimit) throws IOException {
        this.baseDirectory = baseDirectory;
        this.memTable = new MemTable(this.baseDirectory, flushLimit);
        this.ssTables = new ArrayList<>();
        Files.walkFileTree(this.baseDirectory.toPath(), new SimpleFileVisitor<>(){
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                ssTables.add(new SSTable(file.toFile()));
                return FileVisitResult.CONTINUE;
            }
        });
    }


    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List <Iterator<Cluster>> list = new ArrayList<>();
        ssTables.forEach(ssTable -> list.add(ssTable.iterator(from)));
        final Iterator<Cluster> memIter = memTable.iterator(from);
        list.add(memIter);
        Iterator <Cluster> clusterIterator = Iterators.mergeSorted(list, Cluster.COMPARATOR);
        clusterIterator = Iters.collapseEquals(clusterIterator, Cluster::getKey);
        final Iterator <Cluster> alive = Iterators.filter(
                clusterIterator, cluster-> cluster == null || cluster.getClusterValue() == null
                || !cluster.getClusterValue().isTombstone());
        return Iterators.transform(alive, cluster-> {
            assert  cluster != null && cluster.getKey() != null;
            return Record.of(cluster.getKey(), cluster.getClusterValue().getValue());
        });
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        //WriteToFileHelper.writeToFile(memTable.iterator(ByteBuffer.allocate(0)), baseDirectory, memTable.getGeneration());
    }
}
