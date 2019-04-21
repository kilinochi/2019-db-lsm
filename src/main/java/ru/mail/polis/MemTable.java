package ru.mail.polis;


import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.mail.polis.DAOFactory.MAX_HEAP;

class MemTable {

    private NavigableMap<ByteBuffer, Record> storage;
    private File file;
    private static MemTable instance;
    private long tableSize;


    static MemTable entity (File file) {
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
    Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return storage.tailMap(from).values().iterator();
    }


    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        tableSize = tableSize + key.capacity() + value.capacity();
        if(tableSize >= MAX_HEAP) {
            //Todo - reset data to SSTable
            storage.clear();
        }
        storage.put(key, Record.of(key, value));
    }

    void remove(@NotNull ByteBuffer key) throws IOException {
        storage.remove(key);
    }

    private MemTable(File file) {
        this.file = file;
        storage = new ConcurrentSkipListMap<>();
    }
}