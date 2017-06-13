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

	private int	recordScan	= 0;

	private int	recordDeal	= 0;

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getContext());
			logger.info("线程 {} 执行耗时: {},总扫描记录数 {},需要重放的记录数 {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime, recordScan, recordDeal);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(ReadRecordLogContext readRecordLogContext, Context context)
			throws Exception {

		RecordLogReceiver receiver = context.getReceiver();

		RecalculateContext recalculateContext = context.getRecalculateContext();

		String tableSchema = context.getTableSchema();

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader channelReader = ChannelReader.get();

		ReadChannel channel = readRecordLogContext.getChannel();

		for (; channel.hasRemaining();) {

			RecordLog r = channelReader.read(channel, tableSchemaBytes, 8);

			recordScan++;

			if (recordScan % 100000 == 0) {
				System.out.println(recordScan);
			}

			if (r == null) {
				continue;
			}

			recordDeal++;

			context.setTable(Table.newTable(r));

			receiver.received(recalculateContext, r);

			break;
		}

		int cols = context.getTable().getColumnSize();

		for (; channel.hasRemaining();) {

			RecordLog r = channelReader.read(channel, tableSchemaBytes, cols);
			recordScan++;
			if (r == null) {
				continue;
			}
			recordDeal++;
			receiver.received(recalculateContext, r);
		}

	}

}
