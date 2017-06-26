package com.alibaba.middleware.race.sync.channel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 *
 */
public class MuiltFileInputStream extends InputStream {

	private InputStream[]		inputStreams;

	private InputStream			current;

	private int				currentIndex;

	private boolean			hasRemaining	= true;
	
	private ByteBuf			remain;

	public MuiltFileInputStream(InputStream[] inputStreams) {
		this.inputStreams = inputStreams;
		this.current = inputStreams[currentIndex++];
		this.remain = UnpooledByteBufAllocator.getHeapInstance().allocate(1024);
	}

	public int readFull(ByteBuffer buf, int limit) throws IOException{
		ByteBuf remain = this.remain;
		int read = remain.position();
		if (remain.position() > 0) {
			for (int i = remain.position() -1; i >= 0 ; i--) {
				buf.put(remain.getByte(i));
			}
			remain.clear();
		}
		byte [] array = buf.array();
		for(;read < limit;){
			int len = read(array, read, limit - read);
			if (len == -1) {
				buf.position(read);
				return read;
			}
			read += len;
		}
		for(;;){
			byte b = buf.get(--read);
			if (b == '\n') {
				buf.position(++read);
				return read;
			}
			remain.putByte(b);
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
