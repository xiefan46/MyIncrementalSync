package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.alibaba.middleware.race.sync.util.LoggerUtil;
import com.generallycloud.baseio.common.Logger;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;

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
			logger.info("总耗时：{}",(System.currentTimeMillis()-startTime));
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
		int blockSize = context.getBlockSize();
		parseThreads = new ParseThread[parseThreadNum];
		for (int i = 0; i < parseThreadNum; i++) {
			parseThreads[i] = new ParseThread(context, i,(int)(blockSize / 80));
		}
		for (int i = 0; i < parseThreadNum; i++) {
			parseThreads[i].start();
		}
	}

	private void startRecaler(Context context) throws InterruptedException {
		context.getDispatcher().start();
	}

	private Object parseLock = new Object();
	
	private Context		context;
	
	private volatile boolean	waitParseDone = false;
	
	private ReaderThread	readerThread;

	private ParseThread[]	parseThreads;

	private CountDownLatch	recalCountDownLatch;
	
	private AtomicInteger	workDone = new AtomicInteger(0);

	private void execute1(Context context) throws Exception {
		
		MultiFileInputStream channel = initChannels2();
		context.setReadChannel(channel);
		
		startRecaler(context);
		startParser(context);
		startReader(context);
		
		long time1 = 0;
		long time2 = 0;
		
		AtomicInteger	workDone = this.workDone;
		ParseThread[] parseThreads = this.parseThreads;
		int parseThreadNums = parseThreads.length;
		Dispatcher dispatcher = context.getDispatcher();
		for (;parseThreadNums != workDone.get();) {
			long startTime = System.currentTimeMillis();
			int parseIndex = 0;
			recalCountDownLatch = new CountDownLatch(context.getRecalThreadNum());
			dispatcher.beforeDispatch();
			for (;;) {
				if (!parseThreads[parseIndex].isDone()) {
					synchronized (parseLock) {
						if (!parseThreads[parseIndex].isDone()) {
							waitParseDone = true;
							parseLock.wait(1);
						}
					}
					continue;
				}
				ParseThread p = parseThreads[parseIndex++];
				dispatcher.dispatch(p.getResult());
				p.startWork();
				if (parseIndex == context.getParseThreadNum()) {
					break;
				}
			}
			time1 += (System.currentTimeMillis() - startTime);
			startTime = System.currentTimeMillis();
			dispatcher.startWork();
			recalCountDownLatch.await();
			time2 += (System.currentTimeMillis() - startTime);
		}
		logger.info("读取,解析,分发完成:{}，合并完成:{}", time1, time2);
//		PkCount.get().printResult();
		for (ParseThread t : parseThreads) {
			t.shutdown();
		}
		dispatcher.readRecordOver();
	}
	
	public void setWorkDone(int index) {
		this.workDone.getAndIncrement();
	}

	public Context getContext() {
		return context;
	}
	
	public void parseDone(int index){
		if (waitParseDone) {
			synchronized (parseLock) {
				parseLock.notify();
				waitParseDone = false;
			}
		}
	}

	public void recalDone(int index) {
		recalCountDownLatch.countDown();
	}

}
