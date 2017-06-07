package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.util.LoggerUtil;
import org.slf4j.Logger;

import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 */
public class ReadRecordLogThread implements Runnable {

	private static final Logger	logger	= LoggerUtil.getServerLogger();

	private Context			context;

	public ReadRecordLogThread(Context context) {
		this.context = context;
	}

	private int count = 0;

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getTableSchema(), context.getStartId(), context.getEndId());
			logger.info("线程 {} 解析数据完成，用时 : {}. Context中Record数目 : {}. 扫描处理记录条目数:{}",
					Thread.currentThread().getId(), System.currentTimeMillis() - startTime,
					context.getRecords().size(), count);
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

		int all = 0;

		for (; channel.hasRemaining();) {

			Record r = channelReader.read(channel, tableSchemaBytes);

			count++;

			if (r == null) {
				continue;
			}

			all++;

			/*
			 * logger.debug("Alter type : " + r.getAlterType());
			 * 
			 * if (r.getAlterType() == Record.INSERT) {
			 * logger.debug("Receive insert record. PK : {}",
			 * r.getPrimaryColumn().getValue()); }
			 */
			if (Constants.COLLECT_STAT) {
				context.getStat().dealRecord(r);
			} else {
				//r.setTableSchema(tableSchema);
				recordLogReceiver.received(context, r, startId, endId);
			}

		}

		logger.info("lines:{}", all);
	}

}
