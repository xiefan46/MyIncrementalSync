package com.alibaba.middleware.race.sync.map;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.service.IReplayMap;

/**
 * Created by wubincen on 2017/6/24.
 */
public class NormalMap implements IReplayMap {

    private Map<Long, Integer> map = new HashMap<>();
    private List<ByteBuffer> bufferList = new ArrayList<>();
    private final static int SIZE_SHIFT = 20;
    private final static int MASK = (1<<SIZE_SHIFT) - 1;
    private ByteBuffer currentBuffer = ByteBuffer.allocate(1<<SIZE_SHIFT);
    private int position = 0;
    private final int RECORD_SIZE = Context.getInstance().RECORD_SIZE;

    public NormalMap() {
        bufferList.add(currentBuffer);
    }

    @Override
    public int addPk(long pk) {
        if (position + RECORD_SIZE > currentBuffer.capacity()) {
            currentBuffer = ByteBuffer.allocate(1<<SIZE_SHIFT);
            bufferList.add(currentBuffer);
            position = 0;
        }
        int ret = (bufferList.size() - 1) << SIZE_SHIFT | position;
        map.put(pk, ret);
        return ret;
    }

    @Override
    public void remove(long pk) {
        map.remove(pk);
    }

    @Override
    public void update(int offset, int index, long val) {
        bufferList.get(offset >> SIZE_SHIFT).putLong((offset&MASK) + (index << 3), val);
    }

    @Override
    public void update(int offset, int index, byte[] buf, int off, int len) {
        ByteBuffer buffer = bufferList.get(offset >> SIZE_SHIFT);
        buffer.position((offset & MASK) + (index << 3));
        buffer.putShort((short) len);
        buffer.put(buf, off, len);
    }

    @Override
    public int getOffset(long pk) {
        return map.get(pk);
    }
}
