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
			execute(context, context.getContext());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(ReadRecordLogContext readRecordLogContext, Context context)
			throws Exception {

		RecordLogReceiver	receiver = context.getReceiver();
		
		RecalculateContext recalculateContext = context.getRecalculateContext();
		
		String tableSchema = context.getTableSchema();

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader channelReader = ChannelReader.get();

		ReadChannel channel = readRecordLogContext.getChannel();

		int all = 0;
		
		for (; channel.hasRemaining();) {

			RecordLog r = channelReader.read(channel, tableSchemaBytes);

			if (r == null) {
				continue;
			}

			all++;

			context.setTable(Table.newTable(r));

//			context.dispatch(r);
			receiver.received(recalculateContext, r);
			
			break;
		}

		for (; channel.hasRemaining();) {

			RecordLog r = channelReader.read(channel, tableSchemaBytes);

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

//			context.dispatch(r);
			receiver.received(recalculateContext, r);
		}

		logger.info("lines:{}", all);
	}

}
