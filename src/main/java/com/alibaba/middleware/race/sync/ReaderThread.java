package com.alibaba.middleware.race.sync;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;
import com.generallycloud.baseio.common.ThreadUtil;

/**
 * @author wangkai
 */
public class ReaderThread extends Thread {

	private Logger			logger	= LoggerFactory.getLogger(getClass());

	private Context		context;

	private ParseThread[]	parseThreads;

	private CountDownLatch	recalCountDownLatch;
	
	private boolean[]		parseDone;
	
	private Dispatcher		dispatcher;

	public ReaderThread(Context context) {
		this.context = context;
	}

	public void init() throws InterruptedException {
		int threadNum = context.getParseThreadNum();
		int blockSize = context.getBlockSize();
		parseThreads = new ParseThread[threadNum];
		parseDone = new boolean[context.getParseThreadNum()];
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
		boolean [] parseDone = this.parseDone;
		ParseThread[] parseThreads = this.parseThreads;
		Dispatcher dispatcher = this.dispatcher;
		for (; channel.hasRemaining();) {
			long startTime = System.currentTimeMillis();
			int parseIndex = 0;
			recalCountDownLatch = new CountDownLatch(context.getRecalThreadNum());
			for (int i = 0; i < parseThreads.length; i++) {
				parseDone[i] = false;
				ParseThread t = parseThreads[i];
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
			dispatcher.beforeDispatch();
			for(;;){
				if(!parseDone[parseIndex]){
					ThreadUtil.sleep(1);
					continue;
				}
				dispatcher.dispatch(parseThreads[parseIndex++]);
				if (parseIndex == parseDone.length) {
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
		for (ParseThread t : parseThreads) {
			t.shutdown();
		}
		context.getDispatcher().readRecordOver();
	}

	public Context getContext() {
		return context;
	}

	public void parseDone(int index) {
		parseDone[index] = true;
	}
	
	public void recalDone(int index) {
		recalCountDownLatch.countDown();
	}

}
