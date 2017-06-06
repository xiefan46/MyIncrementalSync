package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.alibaba.middleware.race.sync.other.bytes.ByteBufUtil;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.component.ByteArrayInputStream;

/**
 * @author wangkai
 *
 */
public class CompoundReadChannel extends ReadChannel {

	private List<InputStream>	inputStreams;

	private InputStream			current;

	private int				currentIndex;

	private boolean			hasRemaining		= true;

	private boolean			hasTailRemaining	= true;

	private CompoundReadChannel	lastChannel;

	private ByteArrayInputStream	tail;

	private long				remaining;

	public CompoundReadChannel(List<InputStream> inputStreams, int maxBufferLen, long remaining) {
		super(maxBufferLen);
		this.inputStreams = inputStreams;
		this.current = inputStreams.get(currentIndex++);
		this.remaining = remaining;
	}

	public int read(ByteBuf buf) throws IOException {
		if (!hasRemaining) {
			if (!hasTailRemaining) {
				return -1;
			}
			int len = ByteBufUtil.read(buf, tail);
			if (len == -1) {
				hasTailRemaining = false;
				return -1;
			}
			return len;
		}
		int read = (int) Math.min(remaining, buf.capacity());
		int len = ByteBufUtil.read(buf, current, read);
		if (len == -1) {
			if (currentIndex == inputStreams.size()) {
				hasRemaining = false;
				return read(buf);
			}
			this.current = inputStreams.get(currentIndex++);
			return read(buf);
		}
		remaining -= len;
		if (remaining == 0) {
			hasRemaining = false;
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

	public void setTail(byte[] tail) {
		this.tail = new ByteArrayInputStream(tail);
	}

	public CompoundReadChannel getLastChannel() {
		return lastChannel;
	}

	public void setLastChannel(CompoundReadChannel lastChannel) {
		this.lastChannel = lastChannel;
	}

}
