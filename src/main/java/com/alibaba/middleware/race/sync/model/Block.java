package com.alibaba.middleware.race.sync.model;

import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * Created by xiefan on 6/23/17.
 */
public class Block {
    private int id;

    private ByteBuf data;

    public Block(int id, ByteBuf data) {
        this.id = id;
        this.data = data;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ByteBuf getData() {
        return data;
    }

    public void setData(ByteBuf data) {
        this.data = data;
    }
}
