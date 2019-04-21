package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class CustomDAO implements DAO {

    private final MemTable memTable;
    private final File data;

    CustomDAO(File data) {
        this.data = data;
        memTable = MemTable.entity(data);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return memTable.iterator(from);
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
