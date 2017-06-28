package com.alibaba.middleware.race.sync;

import com.generallycloud.baseio.buffer.ByteBuf;

public class ReadTask {
	
	public static final ReadTask END_TASK = new ReadTask(null);
	
	public ReadTask(ByteBuf buf) {
		this.buf = buf;
	}

	private ByteBuf buf;
	
	private short version;

	public ByteBuf getBuf() {
		return buf;
	}

	public short getVersion() {
		return version;
	}

	public void setVersion(short version) {
		this.version = version;
	}
	
}
