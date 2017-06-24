package com.alibaba.middleware.race.sync.common;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.Context;

/**
 * Created by wubincen on 2017/6/19.
 */
public class ReplayMap {
    private static Context context = Context.getInstance();
    private Map<Long, Long> offsetMap = new HashMap<>();
    private Map<Integer, Segment> segmentMap = new HashMap<>();
    private SegmentPool segmentPool = Context.getInstance().getSegmentPool();
    private Segment currentSegment = segmentPool.newSegment();

    public ReplayMap() {
        segmentMap.put(currentSegment.getId(), currentSegment);
    }

    public long addPk(long pk) {
        long offset = currentSegment.newBlock();
        if (offset == -1) {
            currentSegment = segmentPool.newSegment();
            segmentMap.put(currentSegment.getId(), currentSegment);
            offset = currentSegment.newBlock();
        }
        offsetMap.put(pk, offset);
        return offset;
    }

    public long getOffset(long pk) {
        return offsetMap.get(pk);
    }

    private int getSegmentId(long offset) {
        return (int) (offset >> 32);
    }

    private Segment getSegment(long offset) {
        return segmentMap.get(getSegmentId(offset));
    }

    private int getOffsetInSegment(long offset) {
        return (int) (offset & 0x7fffffff);
    }

    public void updateByOffset(long offset, int index, long val) {
        Segment segment = getSegment(offset);
        int offInSegment = getOffsetInSegment(offset);
        segment.put(offInSegment, index, val);
    }

    /**
     * 更新offset中的某一列,该列类型是字符串
     * @param offset
     * @param index
     * @param buf
     * @param off
     * @param len
     */
    public void updateByOffset(long offset, int index, byte[] buf, int off, int len) {
        Segment segment = getSegment(offset);
        int offInSegment = getOffsetInSegment(offset);
        segment.put(offInSegment, index, buf, off, len);
    }

    public void remove(long pk) {
        offsetMap.remove(pk);
    }

    /**
     * 主键变更时,将旧主键的值全部复制给新主键
     * @param pk 新主键
     * @param outOffset 旧主键val的offset
     */
    public void copy(long pk, long outOffset) {
        long offset = addPk(pk);
        int segmentId = getSegmentId(outOffset);
        Segment segment = segmentPool.getSegment(segmentId);
        int offInSegment = getOffsetInSegment(outOffset);
        currentSegment.put(getOffsetInSegment(offset), segment.getbuf(), offInSegment, context.RECORD_SIZE);
    }

    public void getValue(long offset, ByteBuffer buffer) {
        Segment segment = getSegment(offset);
        int offInSegment = getOffsetInSegment(offset);
        buffer.put(segment.getbuf(), offInSegment, context.RECORD_SIZE);
    }

    public Map<Long, Long> getOffsetMap() {
        return offsetMap;
    }
}
