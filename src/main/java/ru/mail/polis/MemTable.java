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
    private static MemTable instance;
    private long tableSize;
    private SSTable ssTable;

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


    void upsert(@NotNull ByteBuffer key, Record value) throws IOException {
        tableSize = tableSize + value.getValue().remaining();
        if(tableSize >= MAX_HEAP) {
            ssTable.upsert(storage);
            storage.clear();
            tableSize = 0;
        }
        storage.put(key, value);
    }

    void remove(@NotNull ByteBuffer key) throws IOException {
        storage.remove(key);
    }

    private MemTable(File file) {
        ssTable = SSTable.entity(file);
        storage = new ConcurrentSkipListMap<>();
    }
}