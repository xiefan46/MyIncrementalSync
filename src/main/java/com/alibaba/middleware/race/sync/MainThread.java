package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.generallycloud.baseio.common.ThreadUtil;

/**
 * @author wangkai
 */
public class MainThread {

	private Logger logger = LoggerFactory.getLogger(getClass());

	public void execute() {
		try {
			logger.info("--------------Main thread start-----------");
			execute1(context);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public MainThread(Context context) {
		this.context = context;
	}

	private MuiltFileInputStream initChannels2() throws IOException {
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
		parseThreads = new ParseThread[parseThreadNum];
		for (int i = 0; i < parseThreadNum; i++) {
			parseThreads[i] = new ParseThread(context, i);
		}
		for (int i = 0; i < parseThreadNum; i++) {
			parseThreads[i].start();
		}
	}

	private void startRecaler(Context context) throws InterruptedException {
		context.getDispatcher().start();
	}

	private Context		context;
	
	private ReaderThread	readerThread;

	private ParseThread[]	parseThreads;

	private CountDownLatch	recalCountDownLatch;
	
	private AtomicInteger	workDone = new AtomicInteger(0);

	private void execute1(Context context) throws Exception {
		//		int tmp = 1024 * 1024 * 1024 / (context.getBlockSize() * context.getParseThreadNum());
		//		int tmpCount = 0;
		
		MuiltFileInputStream channel = initChannels2();
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
					ThreadUtil.sleep(1);
					continue;
				}
				ParseThread p = parseThreads[parseIndex++];
				dispatcher.dispatch(p.getResult(),p.getLimit());
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
		PkCount.get().printResult();
		for (ParseThread t : parseThreads) {
			t.shutdown();
		}
		dispatcher.readRecordOver();
	}
	
	public void setWorkDone() {
		this.workDone.getAndIncrement();
	}

	public Context getContext() {
		return context;
	}

	public void recalDone(int index) {
		recalCountDownLatch.countDown();
	}

}
