package com.alibaba.middleware.race.sync.channel;

import java.io.IOException;
import java.io.InputStream;

import com.alibaba.middleware.race.sync.other.bytes.ByteBufUtil;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 *
 */
public class MuiltFileReadChannel extends ReadChannel {

	private InputStream[]	inputStreams;

	private InputStream		current;

	private int			currentIndex;

	private boolean		hasRemaining	= true;

	public MuiltFileReadChannel(InputStream[] inputStreams, int maxBufferLen) {
		super(maxBufferLen);
		this.inputStreams = inputStreams;
		this.current = inputStreams[currentIndex++];
	}

	public int read(ByteBuf buf) throws IOException {
		if (!hasRemaining) {
			return -1;
		}
		int len = ByteBufUtil.read(buf, current, buf.capacity());
		if (len == -1) {
			if (currentIndex == inputStreams.length) {
				hasRemaining = false;
				return -1;
			}
			this.current = inputStreams[currentIndex++];
			return read(buf);
		}
		return len;
	}

	public InputStream getInputStream() {
		return current;
	}

	public boolean hasRemaining() {
		return hasRemaining;
	}

	@Override
	public int read() throws IOException {
		throw new UnsupportedOperationException();
	}

}
