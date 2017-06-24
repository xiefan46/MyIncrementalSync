package com.alibaba.middleware.race.sync.common;

import java.nio.ByteBuffer;

import com.alibaba.middleware.race.sync.Context;

/**
 * Created by wubincen on 2017/6/18.
 */
public class Segment {
    private ByteBuffer buffer;
    private int id;
    private int offset = 0;

    private static Context context = Context.getInstance();
    private static final int RECORD_SIZE = context.RECORD_SIZE;

    public Segment(int size, int id) {
        buffer = ByteBuffer.allocate(size);
        this.id = id;
    }

    public long newBlock() {
//        return (long)id << 32;
        if (buffer.remaining() < RECORD_SIZE) return -1;
        long ret = (((long) id) << 32) | offset;
        offset += RECORD_SIZE;
        return ret;
    }

    /**
     *
     * @param off segment内的offset,用这个offset定位到5元组
     * @param index 第几列(不包括主键),用index定位到5元组内的具体的某个值
     * @param val
     */
    public void put(int off, int index, long val) {
        buffer.putLong(off + index * 8, val);
    }

    public void put(int off, int index, byte[] buf, int pos, int len) {
        buffer.position(off + index * 8);
        buffer.putShort((short) len);
        buffer.put(buf, pos, len);
    }

    public void put(int off, byte[] buf, int pos, int len) {
        try {
            buffer.position(off);
        }
        catch (Exception ex) {
            System.out.println("fuck pos " + off);
        }
        try {

        buffer.put(buf, pos, len);
        }
        catch (Exception ex) {
            System.out.println("pos " + pos + ", len " + len);
        }
    }

    public int getId() {
        return id;
    }

    public byte[] getbuf() {
        return buffer.array();
    }

}
