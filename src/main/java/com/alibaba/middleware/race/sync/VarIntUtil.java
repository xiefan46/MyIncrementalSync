package com.alibaba.middleware.race.sync;

import java.nio.ByteBuffer;

/**
 * Created by xiefan on 6/24/17.
 */
public class VarIntUtil {
    public static void putVarInt(ByteBuffer buf, int val) {
        int count = 4 * 7;
        while (count > 0) {
            count -= 7;
            byte tmp = (byte) (val >> count);
            if (count == 0) tmp |= (1<<7);
            buf.put(tmp);
            val &= (1<<count) - 1;
        }
    }

    public static int getVarInt(ByteBuffer buf) {
        int ret = 0;
        while (true) {
            byte tmp = buf.get();
            ret = ret << 7 | (tmp & ((1<<7) - 1));
            if ((tmp & (1<<7)) != 0) {
                break;
            }
        }
        return ret;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        ByteBuffer buf = ByteBuffer.allocate(8);
        for (int i = 0; i < (1<<28); ++i) {
            buf.clear();
            putVarInt(buf, i);
            buf.flip();
            if (getVarInt(buf) != i) System.out.println("error");
        }
        long end = System.currentTimeMillis();
        System.out.println("call " + (1<<28) + " times, cost " + (end - start) + " ms");
    }
}
