package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;

import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.alibaba.middleware.race.sync.util.LoggerUtil;

/**
 * @author wangkai
 */
public class MainThread {

	private Logger logger = LoggerUtil.get();

	public void execute() {
		try {
			long startTime = System.currentTimeMillis();
			logger.info("--------------Main thread start-----------");
			execute1(context);
			logger.info("MainThread总耗时：{}",(System.currentTimeMillis()-startTime));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public MainThread(Context context) {
		this.context = context;
	}

	private MultiFileInputStream initChannels2() throws IOException {
		File root = new File(Constants.DATA_HOME);
		return MuiltFileReadChannelSplitor.newInputStream(root.getAbsolutePath() + "/", 1, 10,
				1024 * 256);
	}
	
	private void startReader(Context context) {
		readerThread = new ReaderThread(context, parseThreads);
		readerThread.start();
	}

	private void startParser(Context context) {
		int parseThreadNum = context.getParseThreadNum();
		countDownLatch = new CountDownLatch(parseThreadNum);
		parseThreads = new ParseThread[parseThreadNum];
		for (int i = 0; i < parseThreadNum; i++) {
			parseThreads[i] = new ParseThread(context, i);
		}
		for (int i = 0; i < parseThreadNum; i++) {
			parseThreads[i].start();
		}
	}

	private Context		context;
	
	private ReaderThread	readerThread;

	private ParseThread[]	parseThreads;

	private CountDownLatch	recalCountDownLatch;
	
	private CountDownLatch	countDownLatch;

	private void execute1(Context context) throws Exception {
		MultiFileInputStream channel = initChannels2();
		context.setReadChannel(channel);
		startParser(context);
		startReader(context);
		countDownLatch.await();
		ParseThread[] parseThreads = this.parseThreads;
//		PkCount.get().printResult();
		for (ParseThread t : parseThreads) {
			t.shutdown();
		}
		readerThread.shutdown();
	}
	
	public void setWorkDone() {
		this.countDownLatch.countDown();
	}

	public Context getContext() {
		return context;
	}

	public void recalDone(int index) {
		recalCountDownLatch.countDown();
	}

}
