package com.alibaba.middleware.race.sync.structure;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.alibaba.middleware.race.sync.Constants;

/**
 * Created by xiefan on 6/24/17.
 */
public class Partition {
    private ByteBuffer buffer;
    private int id;
    private RandomAccessFile raf;
    public Partition(int id, int size, String attr) {
        this.id = id;
        buffer = ByteBuffer.allocate(size);
        try {
            raf = new RandomAccessFile(String.format("%s/%s%03d.dat", Constants.MIDDLE_HOME, attr, id), "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

//    public synchronized void put(long val) {
//        if (buffer.remaining() < 8) {
//            spill();
//        }
//        buffer.putLong(val);
//    }

    public synchronized void put(byte[] buf, int off, int len) {
        if (buffer.remaining() < len) {
            spill();
        }
        buffer.put(buf, off, len);
    }

    public synchronized void put(ByteBuffer buf) {
        if (buffer.remaining() < buf.limit()) {
            spill();
        }
        buffer.put(buf);
    }

    private void spill() {
        buffer.flip();
//        try {
//            raf.getChannel().write(buffer);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        buffer.clear();
    }

    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
