package com.alibaba.middleware.race.sync.channel;

import java.io.IOException;
import java.io.InputStream;

import com.alibaba.middleware.race.sync.util.ByteBufUtil;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.common.CloseUtil;

/**
 * @author wangkai
 *
 */
public class SimpleReadChannel extends ReadChannel {

	private InputStream	inputStream;

	private boolean	hasRemaining	= true;

	public SimpleReadChannel(InputStream inputStream, int maxBufferLen) {
		super(maxBufferLen);
		this.inputStream = inputStream;
	}

	public int read(ByteBuf buf) throws IOException {
		int len = ByteBufUtil.read(buf, inputStream);
		if (len == -1) {
			hasRemaining = false;
		}
		return len;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public boolean hasRemaining() {
		return hasRemaining;
	}

	@Override
	public int read() throws IOException {
		return inputStream.read();
	}

	public void close() {
		CloseUtil.close(inputStream);
	}

}
