package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.*;
import com.alibaba.middleware.race.sync.Constants;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

/**
 * Created by xiefan on 6/11/17.
 */
public class Tester {

	private File				root		= new File(Constants.DATA_HOME);

	private static final Logger	logger	= LoggerFactory.getLogger(Tester.class);

	public void testMbb() throws Exception {
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= 10; i++) {
			RandomAccessFile raf = new RandomAccessFile(
					root.getAbsolutePath() + "/" + i + ".txt", "r");
			MappedByteBuffer mbb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0,
					raf.length());
			byte[] bytes = new byte[4096];
			while (mbb.hasRemaining()) {
				if (mbb.remaining() < 4096) {
					mbb.get(bytes, 0, mbb.remaining());
				} else {
					mbb.get(bytes);
				}
			}
		}
		logger.info("mbb 读文件消耗时间 : {}", System.currentTimeMillis() - startTime);
	}

	public void testHelper() throws Exception {
		new PageCacheHelper().run();
	}
}
