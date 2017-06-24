package com.alibaba.middleware.race.sync.common;

/**
 * Created by wubincen on 2017/6/12.
 */
public class OPCode {
    public static final byte INSERT = 1;
    public static final byte UPDATE = 2;
    public static final byte UPDATE_PK = 3;
    public static final byte DELETE = 4;
    public static final byte WAIT = 5;

    public static final byte END = -1;
}
