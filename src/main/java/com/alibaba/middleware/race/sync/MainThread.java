package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.RAFInputStream;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.channel.SimpleReadChannel;

/**
 * @author wangkai
 */
public class MainThread {

	private Logger logger = LoggerFactory.getLogger(getClass());

	public void execute(Context context) {
		try {
			execute1(context);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void execute1(Context context) throws Exception {
		long startTime = System.currentTimeMillis();

		ReadChannel channels = initChannels2();

		ReadRecordLogContext readRecordLogContext = new ReadRecordLogContext(channels, context);

		ReadRecordLogThread readRecordLogThread = new ReadRecordLogThread(readRecordLogContext);

		//		new Thread(context.getRecalculateThread()).start();

		readRecordLogThread.run();

		//		context.stopRecalculateThreads();
		//
		//		context.getRecalculateThread().getCountDownLatch().await();

		logger.info("解析记录耗时 : {}", System.currentTimeMillis() - startTime);
	}

	private ReadChannel initChannels2() throws IOException {
		File root = new File(Constants.DATA_HOME);
		logAllFile(root);
		return MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/", 1, 10,
				1024 * 128);
	}

	private ReadChannel initChannels3() throws IOException {
		File root = new File(Constants.TESTER_HOME + "/canal.txt");
		RandomAccessFile raf = new RandomAccessFile(root, "r");
		RAFInputStream inputStream = new RAFInputStream(raf);
		return new SimpleReadChannel(inputStream, 1024 * 128);
	}

	private void logAllFile(File root) {
		try {
			File[] files = root.listFiles();
			StringBuilder sb = new StringBuilder();
			sb.append("[");
			for (File f : files) {
				sb.append(f.getPath() + " ");
			}
			sb.append("]");
			logger.info("All files : {} ", sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
