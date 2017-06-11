package com.alibaba.middleware.race.sync;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.common.ReleaseUtil;

/**
 * @author wangkai
 *
 */
public class PageCacheHelper implements Runnable {

	private Logger	logger	= LoggerFactory.getLogger(getClass());

	private int	count	= 0;

	@Override
	public void run() {
		readSingleThread();
	}

	public void readSingleThread() {
		logger.info("开始预读数据");
		try {
			long startTime = System.currentTimeMillis();

			File root = new File(Constants.DATA_HOME);
			ReadChannel channel = MuiltFileReadChannelSplitor
					.newChannel(root.getAbsolutePath() + "/", 1, 10, 1024 * 1024);

			ByteBuf buf = channel.getByteBuf();

			int size = channel.read(buf);
			while (size > 0) {
				size = channel.read(buf);
				buf.clear();
			}

			ReleaseUtil.release(buf);

			logger.info("预读数据完成：{}", (System.currentTimeMillis() - startTime));

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void readMultiThread() throws Exception {
		logger.info("开始预读数据");
		long startTime = System.currentTimeMillis();
		ExecutorService service = Executors
				.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		final File root = new File(Constants.DATA_HOME);
		final CountDownLatch latch = new CountDownLatch(10);
		for (int i = 0; i < 10; i++) {
			final int id = i + 1;
			service.submit(new Runnable() {
				@Override
				public void run() {
					try {
						long startTime = System.currentTimeMillis();
						ReadChannel channel = MuiltFileReadChannelSplitor.newChannel(
								root.getAbsolutePath() + "/", id, 1, 1024 * 128);
						ByteBuf buf = channel.getByteBuf();
						while (true) {
							if (!channel.hasRemaining()) {
								break;
							}
							channel.read(buf);
							buf.clear();
						}
						buf.clear();
						ReleaseUtil.release(buf);
						latch.countDown();
						logger.info("线程 : {} , 预读数据完成. 时间 : {}",
								Thread.currentThread().getId(),
								System.currentTimeMillis() - startTime);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}

		latch.await();
		logger.info("全部预读完成,耗时 : {}", System.currentTimeMillis() - startTime);
	}

}
