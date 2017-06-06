package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 */
public class ReadRecordLogThread implements Runnable {

	private Logger		logger	= LoggerFactory.getLogger(getClass());

	private Context	context;

	public ReadRecordLogThread(Context context) {
		this.context = context;
	}

	private int count = 0;

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getTableSchema(), context.getStartId(), context.getEndId());
			logger.info("线程 {} 解析数据完成，用时 : {}. Context中Record数目 : {}",
					Thread.currentThread().getId(), System.currentTimeMillis() - startTime,
					context.getRecords().size());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(Context context, String tableSchema, long startId, long endId)
			throws Exception {

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader channelReader = ChannelReader.get();

		ReadChannel channel = context.getChannel();

		RecordLogReceiver recordLogReceiver = context.getReceiver();

		for (; channel.hasRemaining();) {

			Record r = channelReader.read(channel, tableSchemaBytes, startId, endId);

			if (r == null) {
				//logger.debug("null r");
				continue;
			}

			/*
			 * logger.debug("Alter type : " + r.getAlterType());
			 * 
			 * if (r.getAlterType() == Record.INSERT) {
			 * logger.debug("Receive insert record. PK : {}",
			 * r.getPrimaryColumn().getValue()); }
			 */
			count++;

			if (count % 100000 == 0) {
				logger.info("Thread id : {}. Parse record num : {}",
						Thread.currentThread().getId(), count);
			}

			r.setTableSchema(tableSchema);

			recordLogReceiver.received(context, r);
		}
	}

}
