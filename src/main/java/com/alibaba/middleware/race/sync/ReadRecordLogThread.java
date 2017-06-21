package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.dis.RecordLogEventProducer;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 */
public class ReadRecordLogThread {

	private Logger				logger		= LoggerFactory.getLogger(getClass());

	private int				recordScan	= 0;

	private int				pkUpdate		= 0;

	private RecordLogEventProducer recordLogEventProducer;

	public void execute(Context context) {
		try {
			long startTime = System.currentTimeMillis();
			execute0(context, context.getDispatcher());
			logger.info("线程 {} 执行耗时: {},总扫描记录数 {},pk update {}", Thread.currentThread().getId(),
					System.currentTimeMillis() - startTime, recordScan, pkUpdate);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void execute0(Context context, final Dispatcher dispatcher) throws Exception {

		logger.info("process {}",context.getAvailableProcessors());
		
		long start = System.currentTimeMillis();

		String tableSchema = context.getTableSchema();

		final byte[] tableSchemaBytes = tableSchema.getBytes();

		final ChannelReader2 channelReader = ChannelReader2.get();

		final ReadChannel channel = context.getChannel();
		
		dispatcher.start();
		
		Table table = context.getTable();
		
		int cols = table.getColumnSize();
		
		for(;channel.hasRemaining();){
			RecordLog r = RecordLog.newRecordLog(cols);
			if(!channelReader.read(table, channel, tableSchemaBytes, r)){
				continue;
			}
			dispatcher.dispatch(r);
		}
		
		dispatcher.readRecordOver();
		
		logger.info("读取并分发所有记录耗时 : {} .", System.currentTimeMillis() - start);
	}

	/**
	 * @return the recordLogEventProducer
	 */
	public RecordLogEventProducer getRecordLogEventProducer() {
		return recordLogEventProducer;
	}

}
