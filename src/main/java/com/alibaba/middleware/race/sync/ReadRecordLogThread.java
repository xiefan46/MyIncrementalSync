package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 */
public class ReadRecordLogThread implements Runnable {

	private Logger				logger	= LoggerFactory.getLogger(getClass());

	private Context			context;

	public ReadRecordLogThread(Context context) {
		this.context = context;
	}

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getReadChannel());
			logger.info("线程 {} 执行耗时: {}", Thread.currentThread().getId(), System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(Context context,ReadChannel channel) throws Exception {

		RecordLogReceiver receiver = context.getReceiver();

		RecalculateContext recalculateContext = context.getRecalculateContext();

		String tableSchema = context.getTableSchema();

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader2 channelReader = ChannelReader2.get();
		
		RecordLog r = new RecordLog();

		Table table = context.getTable();
		r.newColumns(table.getColumnSize());
		for (; channel.hasBufRemaining();) {
			r.reset();
			if (!channelReader.read(table,channel, tableSchemaBytes, r)) {
				continue;
			}
			receiver.received(recalculateContext, r);
		}

	}

}
