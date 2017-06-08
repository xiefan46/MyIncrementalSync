package com.alibaba.middleware.race.sync.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.MappedByteBuffer;
import java.util.List;

/**
 * Created by xiefan on 6/5/17.
 */
public class LogicFileInputStream extends InputStream {

	private List<MappedByteBuffer>	mbbs;

	private int					curOffset	= 0;

	public LogicFileInputStream(List<MappedByteBuffer> mbbs) {
		this.mbbs = mbbs;
	}

	@Override
	public int read() throws IOException {
		while (true) {
			if (mbbs.get(curOffset).hasRemaining()) {
				return mbbs.get(curOffset).get();
			} else {
				curOffset++;
				if (curOffset >= mbbs.size())
					return -1;
			}
		}
	}

	//TODO : 这样能否回收mbb?
	@Override
	public void close() throws IOException {
		for (MappedByteBuffer mbb : mbbs) {
			mbb = null;
		}
	}
}
