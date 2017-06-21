package com.alibaba.middleware.race.sync;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.log.InsertLog;
import com.alibaba.middleware.race.sync.model.Record;
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

	//private ByteBuffer	buffer		= ByteBuffer.allocate((int) (1024 * 1024 * 1024));

	//private int		totalSize		= 0;

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

		String tableSchema = context.getTableSchema();

		byte[] tableSchemaBytes = tableSchema.getBytes();

		ChannelReader2 channelReader = ChannelReader2.get();

		ReadChannel channel = readRecordLogContext.getChannel();

		RecordLog r = new RecordLog();

		r.newColumns();

		AllLog allLog = new AllLog();

		long start = System.currentTimeMillis();

		for (; channel.hasBufRemaining();) {

			channelReader.read(channel, tableSchemaBytes, r);

			recordScan++;

			if (r == null) {
				continue;
			}

			recordDeal++;
			Table table = RecordLogCodec2.get().getTable();
			RecordLogCodec2.get().setTableInit(true);
			context.setTable(table);
			InsertLog.setLen(table.getStrColSize(), table.getNumberColSize());
			allLog.putRecord(r);
			break;
		}

		for (; channel.hasBufRemaining();) {

			r = channelReader.read(channel, tableSchemaBytes, r);
			recordScan++;
			if (r == null) {
				continue;
			}
			recordDeal++;

			if (recordDeal % 5000000 == 0) {
				logger.info("record deal : {}", recordDeal);

			}

			allLog.putRecord(r);

		}

		logger.info("加载所有记录到内存. cost time : {}. Record count : {}. Buffer size : {}. ",
				System.currentTimeMillis() - start, recordDeal, allLog.getBuffer().position());

		start = System.currentTimeMillis();

		Map<Integer, Record> result = allLog.reverseDeal(context.getStartId(), context.getEndId(),
				context.getTable());
		context.setRecords(result);
		logger.info("倒序处理所有记录. cost time {}.Reverse scan count : {}",
				System.currentTimeMillis() - start, allLog.getScanCount());

	}

}
