package ru.mail.polis.persistent;


import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

class SSTable {

    private static SSTable instance;
    private final File file;

    private SSTable(File file) {
        this.file = file;
    }

    public static SSTable entity (File file) {
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

    public void upsert(Iterator <Cluster> clusters) throws IOException {
        FileChannel fileChannel = FileChannel.open(
                file.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

    }
}
