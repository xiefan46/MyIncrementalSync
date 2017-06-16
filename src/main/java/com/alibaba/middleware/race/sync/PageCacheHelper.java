package com.alibaba.middleware.race.sync;

import java.io.File;

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
@Deprecated
public class PageCacheHelper implements Runnable {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void run() {

		try {
			long startTime = System.currentTimeMillis();

			File root = new File(Constants.DATA_HOME);
			ReadChannel channel = MuiltFileReadChannelSplitor
					.newChannel(root.getAbsolutePath() + "/", 1, 10, 1024 * 1024 * 1);

			ByteBuf buf = channel.getByteBuf();

			for (;;) {
				if (!channel.hasRemaining()) {
					break;
				}
				channel.read(buf);
				buf.limit(0);
			}

			ReleaseUtil.release(buf);

			logger.info("预读数据完成：{}", (System.currentTimeMillis() - startTime));

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

	}

}
