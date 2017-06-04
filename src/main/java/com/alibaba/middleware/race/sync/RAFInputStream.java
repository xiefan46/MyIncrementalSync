package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * @author wangkai
 *
 */
public class RAFInputStream extends InputStream {

	private RandomAccessFile raf;

	public RAFInputStream(RandomAccessFile raf) {
		this.raf = raf;
	}

	@Override
	public int read() throws IOException {
		return raf.read();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return raf.read(b, off, len);
	}

	@Override
	public void close() throws IOException {
		raf.close();
	}

}
