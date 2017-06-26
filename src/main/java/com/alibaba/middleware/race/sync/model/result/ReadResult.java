package com.alibaba.middleware.race.sync.model.result;

import java.nio.ByteBuffer;

/**
 * Created by xiefan on 6/24/17.
 */
public class ReadResult {

	private ByteBuffer	buffer;

	private int		id;

	public ReadResult(ByteBuffer buffer, int id) {
		this.buffer = buffer;
		this.id = id;
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	public boolean isEnd() {
		return buffer == null;
	}

	public void setBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
