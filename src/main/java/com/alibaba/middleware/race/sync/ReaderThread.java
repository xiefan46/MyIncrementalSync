package com.alibaba.middleware.race.sync;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.model.Block;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ReaderThread extends Thread {

	private Logger		logger	= LoggerFactory.getLogger(getClass());

	private Context	context;

	public ReaderThread(Context context) {
		this.context = context;
	}

	public void init() {

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

		AtomicInteger blockCount = new AtomicInteger(BufferUtil.MAX_BLOCK_NUM);

		MergeThread mergeThread = new MergeThread();

		Thread thread = new Thread(mergeThread);

		thread.start();

		int id = 0;

		ParserPool parserPool = new ParserPool(context, mergeThread);

		long startTime = System.currentTimeMillis();

		while (channel.hasRemaining()) {
			if (blockCount.get() > 0) {
				ByteBuf buf = BufferUtil.getOneBlock();
				int len = channel.readFull(buf, buf.capacity() - 1024);
				if (len == -1) {
					buf.limit(0);
					continue;
				}
				buf.flip();
				Block block = new Block(id++, buf);
				parserPool.submit(block, blockCount);
				blockCount.decrementAndGet();
			} else {
				Thread.currentThread().sleep(100);
			}

		}

		mergeThread.setStop(true);
		thread.join();

		logger.info("读取完成,耗时 : {}", System.currentTimeMillis() - startTime);
	}

	public Context getContext() {
		return context;
	}

}
