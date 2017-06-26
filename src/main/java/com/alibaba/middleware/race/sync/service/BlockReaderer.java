package com.alibaba.middleware.race.sync.service;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.model.result.ReadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;

/**
 * @author wangkai
 */
public class BlockReaderer extends Thread {

	private Logger		logger	= LoggerFactory.getLogger(getClass());

	private Context	context	= Context.getInstance();

	private ParseStage	parseStage;

	public BlockReaderer(ParseStage parseStage) {
		this.parseStage = parseStage;
	}

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute();
			logger.info("读取完成. 耗时 : {}", System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute() throws Exception {
		BufferPool bufferPool = context.getBlockBufferPool();
		MuiltFileInputStream muiltFileInputStream = context.getMuiltFileInputStream();
		int blockId = 0;
		for (; muiltFileInputStream.hasRemaining();) {
			ByteBuffer buf = bufferPool.getBufferWait();
			/*if (buf == null) {
				Thread.currentThread().sleep(10);
				continue;
			}*/
			buf.clear();
			int len = muiltFileInputStream.readFull(buf, buf.capacity() - 1024);
			if (len <= 0) {
				//logger.info("end");
				parseStage.submit(ReadResult.END_TASK);
				break;
			} else {
				//logger.info("aaa");
				buf.flip();
				parseStage.submit(new ReadResult(buf, blockId++));
				if (blockId % 200 == 0) {
					logger.info("block id : {}", blockId);
				}
			}

		}

	}

	public Context getContext() {
		return context;
	}

}
