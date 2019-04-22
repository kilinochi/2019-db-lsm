package ru.mail.polis;


import java.io.*;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

class SSTable {

    private NavigableMap <ByteBuffer, Record> storage;
    private static SSTable instance;
    private File file;

    private SSTable(File file) {
        this.file = file;
        storage = new ConcurrentSkipListMap<>();
    }

    static SSTable entity (File file) {
        SSTable localInstance = instance;
        if (localInstance == null) {
            synchronized (MemTable.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = localInstance = new SSTable(file);
                }
            }
        }
        return localInstance;
    }

    void upsert(NavigableMap<ByteBuffer, Record> strg) throws IOException {
        this.storage.putAll(strg);
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
        OutputStream outputStream = new FileOutputStream(file);
        outputStream.write(ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array());
        outputStream.write(intBuffer.putInt(storage.size()).array());
        intBuffer.clear();
        /*Todo - write data (maybe with FileChannel?). Consider tombstone
        *
        * */
    }
}
