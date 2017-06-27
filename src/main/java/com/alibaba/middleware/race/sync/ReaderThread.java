package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 */
public class ReaderThread extends WorkThread {

	private Logger			logger	= LoggerFactory.getLogger(getClass());

	private Context		context;

	private ParseThread[]	parseThreads;

	public ReaderThread(Context context, ParseThread[] parseThreads) {
		super("reader-", 0);
		this.context = context;
		this.parseThreads = parseThreads;
		this.setWork(true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.alibaba.middleware.race.sync.WorkThread#work()
	 */
	@Override
	protected void work() throws Exception {
		ParseThread[] parseThreads = this.parseThreads;
		MultiFileInputStream channel = context.getReadChannel();
		ByteBufPool byteBufPool = context.getByteBufPool();
		int parseIndex = 0;
		int count = 0;
		for (; channel.hasRemaining();) {
			ByteBuf buf = byteBufPool.allocate();
			if (buf == null) {
				continue;
			}
			int len = channel.readFull(buf, buf.capacity());
			count++;
			if (!Constants.ON_LINE && (count % 1024) == 0) {
				logger.info("Read block : {}", count);
			}
			if (len == -1) {
				buf.limit(0);
			} else {
				buf.flip();
			}

			parseThreads[parseIndex++].offerBuf(buf);
			if (parseIndex == parseThreads.length) {
				parseIndex = 0;
			}
		}

		ByteBuf empty = UnpooledByteBufAllocator.getHeapInstance().allocate(0);

		for (int i = 0; i < parseThreads.length; i++) {
			parseThreads[i].offerBuf(empty);
		}

		setWork(false);
	}

	public Context getContext() {
		return context;
	}

	@Override
	Logger getLogger() {
		return logger;
	}

}
