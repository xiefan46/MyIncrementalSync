package com.alibaba.middleware.race.sync.channel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * @author wangkai
 *
 */
public class MuiltFileReadChannelSplitor {

	public static MuiltFileReadChannel[] split(File root, int bufferLen) throws IOException {
		MuiltFileReadChannel[] cs = new MuiltFileReadChannel[2];
		cs[0] = newChannel(root.getAbsolutePath() + "/", 0, 5, bufferLen);
		cs[1] = newChannel(root.getAbsolutePath() + "/", 5, 5, bufferLen);
		return cs;
	}

	public static MuiltFileReadChannel newChannel(String path, int begin, int len, int bufferLen)
			throws FileNotFoundException {
		InputStream[] streams = new InputStream[len];
		for (int i = 0; i < len; i++) {
			File file = new File(path + (i + begin) + ".txt");
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			streams[i] = new RAFInputStream(raf);
		}
		return new MuiltFileReadChannel(streams, bufferLen);
	}
	
	public static MuiltFileInputStream newInputStream(String path, int begin, int len, int bufferLen)
			throws FileNotFoundException {
		InputStream[] streams = new InputStream[len];
		for (int i = 0; i < len; i++) {
			File file = new File(path + (i + begin) + ".txt");
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			streams[i] = new RAFInputStream(raf);
		}
		return new MuiltFileInputStream(streams);
	}

}
