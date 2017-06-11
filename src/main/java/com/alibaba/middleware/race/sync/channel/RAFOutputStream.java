package com.alibaba.middleware.race.sync.channel;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * @author wangkai
 *
 */
public class RAFOutputStream extends OutputStream {

	private RandomAccessFile raf;

	public RAFOutputStream(RandomAccessFile raf) {
		this.raf = raf;
	}

	@Override
	public void write(int b) throws IOException {
		raf.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		raf.write(b, off, len);
	}

	@Override
	public void close() throws IOException {
		raf.close();
	}

}
