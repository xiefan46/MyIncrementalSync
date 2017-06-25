package com.alibaba.middleware.race.sync.channel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 *
 */
public class MuiltFileInputStream extends InputStream {

	private InputStream[]		inputStreams;

	private InputStream			current;

	private int				currentIndex;

	private boolean			hasRemaining	= true;

	public MuiltFileInputStream(InputStream[] inputStreams) {
		this.inputStreams = inputStreams;
		this.current = inputStreams[currentIndex++];
	}

	public int readFull(ByteBuffer buf, int limit) throws IOException{
		byte [] b = buf.array();
		int read = 0;
		for(;read < limit;){
			int len = read(b, read, limit - read);
			if (len == -1) {
				return len;
			}
			read += len;
			buf.position(read);
		}
		for(;;){
			int r = read();
			if (r == -1) {
				return read;
			}
			buf.put((byte) r);
			read++;
			if (r == '\n') {
				return read;
			}
		}
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (!hasRemaining) {
			return -1;
		}
		int read = current.read(b, off, len);
		if (read == -1) {
			if (currentIndex == inputStreams.length) {
				hasRemaining = false;
				return -1;
			}
			this.current = inputStreams[currentIndex++];
			return read(b, off, len);
		}
		return read;
	}

	public InputStream getInputStream() {
		return current;
	}

	public boolean hasRemaining() {
		return hasRemaining;
	}

	@Override
	public int read() throws IOException {
		if (!hasRemaining) {
			return -1;
		}
		int read = current.read();
		if (read == -1) {
			if (currentIndex == inputStreams.length) {
				hasRemaining = false;
				return -1;
			}
			this.current = inputStreams[currentIndex++];
			return read();
		}
		return read;
	}

}
