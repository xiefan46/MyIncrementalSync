package com.alibaba.middleware.race.sync.channel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * @author wangkai
 *
 */
public class MuiltFileReadChannelSplitor {

	public static MultiFileInputStream newInputStream(String path, int begin, int len, int bufferLen)
			throws FileNotFoundException {
		InputStream[] streams = new InputStream[len];
		for (int i = 0; i < len; i++) {
			File file = new File(path + (i + begin) + ".txt");
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			streams[i] = new RAFInputStream(raf);
		}
		return new MultiFileInputStream(streams);
	}

}
