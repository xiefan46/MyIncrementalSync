package com.alibaba.middleware.race.sync;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 */
public class ReaderThread extends Thread {

	private Logger			logger	= LoggerFactory.getLogger(getClass());

	private Context		context;

	private ParseThread[]	parseThreads;

	private CountDownLatch	parseCountDownLatch;
	
	private CountDownLatch	recalCountDownLatch;

	private Dispatcher		dispatcher;

	public ReaderThread(Context context) {
		this.context = context;
	}

	public void init() throws InterruptedException {
		int threadNum = context.getParseThreadNum();
		int blockSize = context.getBlockSize();
		parseThreads = new ParseThread[threadNum];
		dispatcher = context.getDispatcher();
		ByteBufAllocator allocator = UnpooledByteBufAllocator.getHeapInstance();
		for (int i = 0; i < threadNum; i++) {
			ByteBuf buf = allocator.allocate(blockSize);
			parseThreads[i] = new ParseThread(this, buf, i);
		}
		for (int i = 0; i < threadNum; i++) {
			parseThreads[i].start();
		}
		dispatcher.start();
	}

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getReadChannel());
			logger.info("线程 {} 执行耗时: {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(Context context, MuiltFileInputStream channel) throws Exception {
//		int tmp = 1024 * 1024 * 1024 / (context.getBlockSize() * context.getParseThreadNum());
//		int tmpCount = 0;
		long time1 = 0;
		long time2 = 0;
		long time3 = 0;
		long time4 = 0;
		for (; channel.hasRemaining();) {
			long startTime = System.currentTimeMillis();
			parseCountDownLatch = new CountDownLatch(context.getParseThreadNum());
			recalCountDownLatch = new CountDownLatch(context.getRecalThreadNum());
			for (ParseThread t : parseThreads) {
				ByteBuf buf = t.getBuf();
				buf.clear();
				int len = channel.readFull(buf, buf.capacity() - 1024);
				if (len == -1) {
					buf.limit(0);
				} else {
					buf.flip();
				}
				t.startWork();
			}
			time1 += (System.currentTimeMillis() - startTime);
			startTime = System.currentTimeMillis();
			parseCountDownLatch.await();
			time2 += (System.currentTimeMillis() - startTime);
			startTime = System.currentTimeMillis();
			dispatcher.dispatch(parseThreads);
			time3 += (System.currentTimeMillis() - startTime);
			startTime = System.currentTimeMillis();
			dispatcher.startWork();
			recalCountDownLatch.await();
			time4 += (System.currentTimeMillis() - startTime);
				
		}
		logger.info("读取完成:{}，解析完成:{},分发完成:{}，合并完成:{}", time1, time2, time3,time4);
		for (ParseThread t : parseThreads) {
			t.shutdown();
		}
		context.getDispatcher().readRecordOver();
	}

	public Context getContext() {
		return context;
	}

	public void parseDone(int index) {
		parseCountDownLatch.countDown();
	}
	
	public void recalDone(int index) {
		recalCountDownLatch.countDown();
	}

}
