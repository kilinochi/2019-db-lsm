package ru.mail.polis.persistent;


import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.mail.polis.DAOFactory.MAX_HEAP;


public class MemTable {

    public static final String SUFFIX = ".dat";
    public static final String TEMP = ".tmp";
    private NavigableMap<ByteBuffer, ClusterValue> storage;
    private static MemTable instance;
    private long tableSize;
    private SSTable ssTable;
    private File base;
    private int generation;

    public static MemTable entity (File file) {
        MemTable localInstance = instance;
        if (localInstance == null) {
            synchronized (MemTable.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = localInstance = new MemTable(file);
                }
            }
        }
        return localInstance;
    }

    @NotNull
    public Iterator<Cluster> iterator(@NotNull ByteBuffer from) throws IOException {
        return Iterators.transform(
                storage.tailMap(from).entrySet().iterator(), e -> {
                    assert e != null;
                    return new Cluster(e.getKey(), e.getValue());
                }
        );
    }

    public void upsert(@NotNull ByteBuffer key, ByteBuffer value) throws IOException {
        final ClusterValue prev = storage.put(key, ClusterValue.of(value));
        if(prev == null) {
            tableSize = tableSize + key.remaining() + value.remaining();
        }
        else if(prev.isTombstone()) {
            tableSize = tableSize + value.remaining();
        }
        else {
            tableSize = tableSize + value.remaining() - prev.getData().remaining();
        }
        if (tableSize >= MAX_HEAP / 3) {
            final File tmp = new File(base, generation + TEMP);
            WriteToFileWrapper.writeToFile(this.iterator(ByteBuffer.allocate(0)), tmp);
            final File dest = new File(base, generation + SUFFIX);
            Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
            generation++;
            storage = null;
            storage = new ConcurrentSkipListMap<>();
            tableSize = 0;
        }
    }

    public void remove(@NotNull ByteBuffer key) throws IOException {
        final ClusterValue prev = storage.put(key, ClusterValue.deadCluster());
        if (prev == null) {
            tableSize = tableSize + key.remaining();
        } else if (prev.isTombstone()) {

        } else {
            tableSize -= prev.getData().remaining();
        }
    }

    private MemTable(File file) {
        this.base = file;
        storage = new ConcurrentSkipListMap<>();
        final Collection<Path> files = new ArrayList<>();
        try {
            Files.walk(file.toPath(), 1).filter(path -> path.getFileName().toString().endsWith(SUFFIX));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}