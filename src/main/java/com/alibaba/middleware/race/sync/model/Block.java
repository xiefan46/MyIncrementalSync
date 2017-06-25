package com.alibaba.middleware.race.sync.model;

import java.nio.ByteBuffer;

/**
 * Created by xiefan on 6/24/17.
 */
public class Block {

	private ByteBuffer	buffer;

	private long		blockId;

	public Block(ByteBuffer buffer, long blockId) {
		this.buffer = buffer;
		this.blockId = blockId;
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	public long getBlockId() {
		return blockId;
	}

	public boolean isEnd() {
		return buffer == null;
	}

	public static final Block END_TASK = new Block(null, -1);
}
