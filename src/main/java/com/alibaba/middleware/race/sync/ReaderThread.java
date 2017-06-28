package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.MultiFileInputStream;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ReaderThread extends WorkThread {

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
		int version = 0;
		for (; channel.hasRemaining();) {
			ReadTask task = byteBufPool.allocate(); 
			if (task == null) {
				continue;
			}
			ByteBuf buf = task.getBuf();
			int len = channel.readFull(buf, buf.capacity());
			if (len == -1) {
				buf.limit(0);
			} else {
				buf.flip();
			}
			task.setVersion(version++);
			parseThreads[parseIndex++].offerTask(task);
			if (parseIndex == parseThreads.length) {
				parseIndex = 0;
			}
		}

		for (int i = 0; i < parseThreads.length; i++) {
			parseThreads[i].offerTask(ReadTask.END_TASK);
		}

		setWork(false);
	}

	public Context getContext() {
		return context;
	}

}
