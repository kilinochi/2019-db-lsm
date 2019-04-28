package ru.mail.polis.persistent;


import com.google.common.primitives.Bytes;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class SSTable {


    private SSTable(final File file) {

    }


    public Iterator<Cluster> iterator(ByteBuffer from) {
        return new Iterator<Cluster>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Cluster next() {
                return null;
            }
        };
    }
}

