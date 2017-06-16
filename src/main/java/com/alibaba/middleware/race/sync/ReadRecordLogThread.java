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

	private ReadRecordLogContext	context;

	public ReadRecordLogThread(ReadRecordLogContext context) {
		this.context = context;
	}

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getContext());
			logger.info("线程 {} 执行耗时: {}", Thread.currentThread().getId(), System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(ReadRecordLogContext readRecordLogContext, Context context) throws Exception {

		RecordLogReceiver receiver = context.getReceiver();

		RecalculateContext recalculateContext = context.getRecalculateContext();

		String tableSchema = context.getTableSchema();

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader2 channelReader = ChannelReader2.get();

		ReadChannel channel = readRecordLogContext.getChannel();

		RecordLog r = new RecordLog();

		r.newColumns(8);

		for (; channel.hasBufRemaining();) {

			channelReader.read(channel, tableSchemaBytes, r);

			if (r == null) {
				continue;
			}

			context.setTable(Table.newTable(r));

			receiver.received(recalculateContext, r);

			break;
		}

		for (; channel.hasBufRemaining();) {

			r = channelReader.read(channel, tableSchemaBytes, r);
			if (r == null) {
				continue;
			}
			receiver.received(recalculateContext, r);
		}

	}

}
