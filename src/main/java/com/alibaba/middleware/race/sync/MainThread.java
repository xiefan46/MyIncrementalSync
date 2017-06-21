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
			logger.info("--------------Main thread start-----------");
			execute1(context);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void execute1(Context context) throws Exception {
		
		long startTime = System.currentTimeMillis();

		ReadChannel channel = initChannels2();
		
		context.setReadChannel(channel);

		ReadRecordLogThread readRecordLogThread = new ReadRecordLogThread(context);

		readRecordLogThread.run();

		logger.info("等待所有线程完成总耗时 : {}", System.currentTimeMillis() - startTime);
		JvmUsingState.print();
	}

	private ReadChannel initChannels2() throws IOException {
		File root = new File(Constants.DATA_HOME);
		return MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/", 1, 10,
				1024 * 256);
	}

	private ReadChannel initChannels3() throws IOException {
		File root = new File(Constants.TESTER_HOME + "/canal.txt");
		RandomAccessFile raf = new RandomAccessFile(root, "r");
		RAFInputStream inputStream = new RAFInputStream(raf);
		return new SimpleReadChannel(inputStream, 1024 * 256);
	}

}
