package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.FileNotFoundException;
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
		File root = new File(Constants.DATA_HOME);
//		File root = new File(Constants.TESTER_HOME+"/canal.txt");
		if (!root.exists()) {
			throw new FileNotFoundException(root.getAbsolutePath());
		}
		
		ReadChannel channels = initChannels2(root);
		
		ReadRecordLogContext readRecordLogContext = new ReadRecordLogContext(channels, context);

		ReadRecordLogThread readRecordLogThread = new ReadRecordLogThread(readRecordLogContext);

		initRecalculateThread(context);
		
		readRecordLogThread.run();
		
		logger.info("MainThread 初始化耗时 : {}", System.currentTimeMillis() - startTime);

		startTime = System.currentTimeMillis();

		context.stopRecalculateThreads();

		logger.info("解析记录耗时 : {}", System.currentTimeMillis() - startTime);

	}

	private void initRecalculateThread(Context context) throws InterruptedException {
		new Thread(context.getRecalculateThread()).start();
	}

	private ReadChannel initChannels2(File root) throws IOException {
		return MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/", 1, 10,
				1024 * 128);
	}
	
	private ReadChannel initChannels3(File root) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(root, "r");
		RAFInputStream inputStream = new RAFInputStream(raf);
		return new SimpleReadChannel(inputStream, 1024 * 128);
	}

}
