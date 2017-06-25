package com.alibaba.middleware.race.sync.map;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.middleware.race.sync.Context;

/**
 * Created by wubincen on 2017/6/24.
 */
public class ArrayRecordMap implements RecordMap {

    private int[] buckets;
    private List<ByteBuffer> bufList = new ArrayList<>();
    private static final int SIZE_SHIFT = 20; // 1M
    private static final int MASK = (1<<SIZE_SHIFT) - 1;
    private ByteBuffer currentBuffer = ByteBuffer.allocate(1<<SIZE_SHIFT);
    private Context context = Context.getInstance();
    private final int RECORD_SIZE = context.RECORD_SIZE;
    private int start;
    private int end;
    private int position = 0;

    public ArrayRecordMap(int start, int end) {
        buckets = new int[end - start + 1];
        bufList.add(currentBuffer);
        this.start = start;
        this.end = end;
        Arrays.fill(buckets, -1);
    }

    @Override
    public int addPk(long pk) {
        if (position + RECORD_SIZE > currentBuffer.capacity()) {
            currentBuffer = ByteBuffer.allocate(1<<SIZE_SHIFT);
            bufList.add(currentBuffer);
            position = 0;
        }
        int ret = (bufList.size() - 1) << SIZE_SHIFT | position;
        position += RECORD_SIZE;
        buckets[(int) (pk-start)] = ret;
        return ret;
    }

    @Override
    public void remove(long pk) {
        buckets[(int) (pk - start)] = -1;
    }

    @Override
    public void update(int offset, int index, long val) {
        bufList.get(offset >> SIZE_SHIFT).putLong((offset & MASK) + (index << 3), val);
    }

    @Override
    public void update(int offset, int index, byte[] buf, int off, int len) {
        ByteBuffer buffer = bufList.get(offset >> SIZE_SHIFT);
        buffer.position((offset & MASK) + (index << 3));
        buffer.putShort((short) len);
        buffer.put(buf, off, len);
    }

    @Override
    public int getOffset(long pk) {
        return buckets[(int) (pk - start)];
    }

    public int[] getBuckets() {
        return buckets;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public void getValue(int offset, ByteBuffer out) {
        ByteBuffer buffer = bufList.get(offset >> SIZE_SHIFT);
        out.put(buffer.array(), offset & MASK, RECORD_SIZE);
    }
}
