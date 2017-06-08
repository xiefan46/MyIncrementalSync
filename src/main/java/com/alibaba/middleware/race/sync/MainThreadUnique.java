package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.CompoundReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.RAFInputStream;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.channel.SimpleReadChannel;

/**
 * @author wangkai
 */
public class MainThreadUnique {

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
		File root = new File(Constants.DATA_HOME);
		if (!root.exists()) {
			throw new FileNotFoundException(root.getAbsolutePath());
		}
		File[] files = root.listFiles();
		logger.debug("files num : " + files.length);
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
		int coreProcesses = context.getAvailableProcessors();
		RecalculateThread[] threads = context.getRecalculateThreads();
		Thread[] ts = new Thread[coreProcesses];
		for (int i = 0; i < ts.length; i++) {
			ts[i] = new Thread(threads[i]);
		}
		for (int i = 0; i < ts.length; i++) {
			ts[i].start();
		}
	}

	private ReadChannel initChannels2(File root) throws IOException {
		return MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/", 0, 10,
				1024 * 128);
	}

}
