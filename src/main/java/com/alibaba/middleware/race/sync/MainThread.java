package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;

import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.RAFInputStream;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.channel.SimpleReadChannel;
import com.alibaba.middleware.race.sync.util.LoggerUtil;

/**
 * @author wangkai
 */
public class MainThread {

	private Logger logger = LoggerUtil.SERVER_LOGGER;
	
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

		new Thread(context.getRecalculateThread()).start();
		
		readRecordLogThread.run();
		
		logger.info("MainThread 初始化耗时 : {}", System.currentTimeMillis() - startTime);

		startTime = System.currentTimeMillis();
		
		context.stopRecalculateThreads();

		context.getRecalculateThread().getCountDownLatch().await();

		logger.info("解析记录耗时 : {}", System.currentTimeMillis() - startTime);

	}

	private ReadChannel initChannels2() throws IOException {
		File root = new File(Constants.DATA_HOME);
		return MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/", 1, 10,
				1024 * 128);
	}
	
	private ReadChannel initChannels3() throws IOException {
		File root = new File(Constants.TESTER_HOME+"/canal.txt");
		RandomAccessFile raf = new RandomAccessFile(root, "r");
		RAFInputStream inputStream = new RAFInputStream(raf);
		return new SimpleReadChannel(inputStream, 1024 * 128);
	}

}
