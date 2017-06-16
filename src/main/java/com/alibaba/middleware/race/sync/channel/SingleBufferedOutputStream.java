package com.alibaba.middleware.race.sync.channel;

import java.io.IOException;
import java.io.OutputStream;

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 *
 */
public class SingleBufferedOutputStream extends OutputStream{

	private ByteBuf		buf;
	
	private OutputStream	outputStream;

	public SingleBufferedOutputStream(OutputStream outputStream, int maxBufferLen) {
		this.outputStream = outputStream;
		this.buf = UnpooledByteBufAllocator.getHeapInstance().allocate(maxBufferLen);
	}

	public void write(byte[] src) throws IOException {
		write(src, 0, src.length);
	}
	
	@Override
	public void write(int b) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void write(byte[] src, int off, int len) throws IOException {
		ByteBuf buf = this.buf;
		if (len > buf.capacity()) {
			write();
			outputStream.write(src, off, len);
			return;
		}
		if (len + buf.position() > buf.capacity()) {
			write();
			write(src, off, len);
			return;
		}
		buf.put(src, off, len);
	}

	public void flush() throws IOException {
		write();
		outputStream.flush();
	}

	public OutputStream getOutputStream() {
		return outputStream;
	}
	
	private void write() throws IOException{
		ByteBuf buf = this.buf;
		buf.flip();
		outputStream.write(buf.array(), buf.position(), buf.limit());
		buf.clear();
	}

	public ByteBuf getBuffer() {
		return buf;
	}
	
	public void writeFirst(byte b){
		buf.array()[0] = b;
		buf.position(1);
	}

	public void close() throws IOException{
		outputStream.close();
	}

}
