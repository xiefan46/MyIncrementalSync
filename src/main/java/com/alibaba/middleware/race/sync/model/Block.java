package com.alibaba.middleware.race.sync.model;

import java.nio.ByteBuffer;

/**
 * Created by xiefan on 6/24/17.
 */
public class Block {

	private ByteBuffer	buffer;

	private int		blockId;

	public Block(ByteBuffer buffer, int blockId) {
		this.buffer = buffer;
		this.blockId = blockId;
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	public boolean isEnd() {
		return buffer == null;
	}

	public static final Block END_TASK = new Block(null, -1);

	public int getBlockId() {
		return blockId;
	}

	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}
}
