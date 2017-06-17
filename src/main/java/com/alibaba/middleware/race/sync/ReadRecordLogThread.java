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

	private int		recordScan	= 0;

	private int		recordDeal	= 0;

	private int		pkUpdate		= 0;

	private Dispatcher	dispatcher	= new Dispatcher(8);

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getContext());
			logger.info("线程 {} 执行耗时: {},总扫描记录数 {},需要重放的记录数 {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime, recordScan, recordDeal);
			logger.info("max_record_len:{}", ChannelReader.get().getMaxRecordLen() + 1);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(ReadRecordLogContext readRecordLogContext, Context context)
			throws Exception {

		long start = System.currentTimeMillis();

		RecordLogReceiver receiver = context.getReceiver();

		String tableSchema = context.getTableSchema();

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader2 channelReader = ChannelReader2.get();

		ReadChannel channel = readRecordLogContext.getChannel();

		RecordLog r = RecordLog.newRecordLog();

		for (; channel.hasBufRemaining();) {

			channelReader.read(channel, tableSchemaBytes, r);

			recordScan++;

			if (r == null) {
				continue;
			}

			recordDeal++;
			if (r.isPKUpdate()) {
				pkUpdate++;
			}

			context.setTable(Table.newTable(r));

			dispatcher.start(r);

			dispatcher.dispatch(r);

			r = RecordLog.newRecordLog();
			//receiver.received(recalculateContext, r);

			break;
		}

		for (; channel.hasBufRemaining();) {

			r = channelReader.read(channel, tableSchemaBytes, r);
			recordScan++;
			if (r == null) {
				continue;
			}
			if (r.isPKUpdate()) {
				pkUpdate++;
			}
			recordDeal++;
			dispatcher.dispatch(r);
			r = RecordLog.newRecordLog();
			//receiver.received(recalculateContext, r);
		}

		logger.info("读取并分发所有记录耗时 : {} . RedirectMap 大小 : {}. Pk更新的log条目 : {}",
				System.currentTimeMillis() - start, dispatcher.getRedirectMap().size(),
				pkUpdate);

		dispatcher.readRecordOver();
		dispatcher.waitForOk(context);

	}

}
