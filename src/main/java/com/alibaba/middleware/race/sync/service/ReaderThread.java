package com.alibaba.middleware.race.sync.service;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.sync.Config;
import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.model.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 */
public class ReaderThread extends Thread {

	private Logger						logger	= LoggerFactory.getLogger(getClass());

	private Context					context;

	private ConcurrentLinkedQueue<Block>	blocksQueue;

	public ReaderThread(Context context, ConcurrentLinkedQueue<Block> blocksQueue) {
		this.context = context;
		this.blocksQueue = blocksQueue;
	}

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, blocksQueue);
			logger.info("线程 {} 执行耗时: {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(Context context, ConcurrentLinkedQueue<Block> blocksQueue)
			throws Exception {
		BufferPool bufferPool = context.getReadBufferPool();
		MuiltFileInputStream muiltFileInputStream = context.getMuiltFileInputStream();
		int blockId = 0;
		for (; muiltFileInputStream.hasRemaining();) {
			ByteBuffer buf = bufferPool.getBuffer();
			if (buf == null) {
				Thread.currentThread().sleep(10);
				continue;
			}
			buf.clear();
			int len = muiltFileInputStream.readFull(buf, buf.capacity() - 1024);
			if (len == -1) {
				blocksQueue.add(Block.END_TASK);
				break;
			} else {
				buf.flip();
				blocksQueue.add(new Block(buf, blockId++));
				if (blockId % 2000 == 0) {
					logger.info("block id : {}", blockId);
				}
			}

		}

	}

	public Context getContext() {
		return context;
	}

}
