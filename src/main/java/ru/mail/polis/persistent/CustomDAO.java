package ru.mail.polis.persistent;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.nio.file.StandardCopyOption;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitOption;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CustomDAO implements DAO {

    private static final String SUFFIX_DAT = ".dat";
    private static final String SUFFIX_TMP = ".tmp";
    private static final String FILE_NAME = "SSTable";
    private static final Pattern WATCH_FILE_NAME = Pattern.compile(FILE_NAME);

    private final File directory;
    private final long flushLimit;
    private MemTable memTable;
    private final List<SSTable> ssTables;
    private int generation;

    /**
     * Creates persistence CustomDAO.
     *
     * @param flushLimit is the limit upon reaching which we write data in disk
     * @param directory is the base directory, where contains our database
     * @throws IOException of an I/O error occurred
     **/

    public CustomDAO(@NotNull final File directory, final long flushLimit) throws IOException {
        this.directory = directory;
        assert flushLimit >= 0L;
        this.flushLimit = flushLimit;
        memTable = new MemTable();
        ssTables = new ArrayList<>();
        Files.walkFileTree(directory.toPath(), EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>(){
            @Override
            public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs)
                    throws IOException {
                    final Matcher matcher = WATCH_FILE_NAME.matcher(path.toString());
                    if(path.toString().endsWith(SUFFIX_DAT) && matcher.find()) {
                        ssTables.add(new SSTable(path.toFile()));
                    }
                    return FileVisitResult.CONTINUE;
            }
        });
    }


    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cluster>> iters = new ArrayList<>();
        for (final SSTable ssTable : this.ssTables) {
            iters.add(ssTable.iterator(from));
        }

        iters.add(memTable.iterator(from));
        final Iterator<Cluster> clusterIterator = Iters.collapseEquals(
                Iterators.mergeSorted(iters, Cluster.COMPARATOR),
                Cluster::getKey
        );
        final Iterator<Cluster> alive = Iterators.filter(
                clusterIterator, cluster -> {
                    assert cluster != null;
                    return !cluster.getClusterValue().isTombstone();
                }
        );
        return Iterators.transform(alive, cluster -> {
            assert cluster != null;
            return Record.of(cluster.getKey(), cluster.getClusterValue().getData());
        });
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.size() >= flushLimit) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.size() >= flushLimit) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if(memTable.size() == 0) {
            return;
        }
        flush();
    }

    private void flush() throws IOException {
        final File tmp = new File(directory, FILE_NAME + generation + SUFFIX_TMP);
        SSTable.writeToFile(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(directory, FILE_NAME + generation + SUFFIX_DAT);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation = generation + 1;
        memTable = new MemTable();
    }
}
