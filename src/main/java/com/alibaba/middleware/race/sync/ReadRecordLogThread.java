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

	private Logger		logger	= LoggerFactory.getLogger(getClass());

	private Context	context;

	public ReadRecordLogThread(Context context) {
		this.context = context;
	}

	private int	recordScan	= 0;

	private int	recordDeal	= 0;

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context);
			logger.info("线程 {} 执行耗时: {},总扫描记录数 {},需要重放的记录数 {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime, recordScan, recordDeal);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(Context context) throws Exception {

		RecordLogReceiver receiver = context.getReceiver();

		String tableSchema = context.getTableSchema();

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader channelReader = ChannelReader.get();

		ReadChannel channel = context.getChannel();

		int cols = context.getTable().getColumnSize();

		for (; channel.hasRemaining();) {

			RecordLog r = channelReader.read(channel, tableSchemaBytes, cols);
			recordScan++;
			if (r == null) {
				continue;
			}
			recordDeal++;
			receiver.received(context, r);
		}

	}

}
