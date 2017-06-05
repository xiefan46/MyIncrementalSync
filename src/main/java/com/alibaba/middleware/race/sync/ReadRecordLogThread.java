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

	@Override
	public void run() {
		try {
			execute(context, context.getTableSchema(), context.getStartId(), context.getEndId());
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
				continue;
			}

			logger.debug("Alter type : " + r.getAlterType());

			if (r.getAlterType() == Record.INSERT) {
				logger.debug("Receive insert record. PK : {}", r.getPrimaryColumn().getValue());
			}

			r.setTableSchema(tableSchema);

			recordLogReceiver.received(context, r);
		}
	}

}
