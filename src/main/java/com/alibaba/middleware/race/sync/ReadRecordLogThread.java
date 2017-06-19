package com.alibaba.middleware.race.sync;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.dis.RecordLogEvent;
import com.alibaba.middleware.race.sync.dis.RecordLogEventFactory;
import com.alibaba.middleware.race.sync.dis.RecordLogEventProducer;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.common.ThreadUtil;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

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
			logger.info("max_record_len:{}", ChannelReader.get().getMaxRecordLen() + 1);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void execute0(Context context, final Dispatcher dispatcher) throws Exception {

		long start = System.currentTimeMillis();

		String tableSchema = context.getTableSchema();

		final byte[] tableSchemaBytes = tableSchema.getBytes();

		final ChannelReader2 channelReader = ChannelReader2.get();

		final ReadChannel channel = context.getChannel();
		
		RecordLog rFirst = RecordLog.newRecordLog(8);

		for (; channel.hasBufRemaining();) {

			channelReader.read(channel, tableSchemaBytes, rFirst);

			if (rFirst == null) {
				continue;
			}

			recordScan++;

			if (rFirst.isPKUpdate()) {
				pkUpdate++;
			}

			context.setTable(Table.newTable(rFirst));

			dispatcher.start(rFirst);

			break;
		}

		RecordLogEventFactory factory = new RecordLogEventFactory();

		ThreadFactory threadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r,"channel-reader");
			}
		};

		final CountDownLatch countDownLatch = new CountDownLatch(1);

		int rSize = context.getRingBufferSize();
		
		Disruptor<RecordLogEvent> disruptor = new Disruptor<>(factory,rSize, threadFactory);

		EventHandler eventHandler = new EventHandler<RecordLogEvent>() {

			@Override
			public void onEvent(RecordLogEvent event, long sequence, boolean endOfBatch)
					throws Exception {

				if (!channel.hasBufRemaining()) {
					countDownLatch.countDown();
					return;
				}

				RecordLog r = event.getRecordLog();
				
				r.reset();

				if (!channelReader.read(channel, tableSchemaBytes, r)) {
					countDownLatch.countDown();
					return;
				}
				recordScan++;
				if (r.isPKUpdate()) {
					pkUpdate++;
				}

				dispatcher.dispatch(r);
			}
		};

		disruptor.handleEventsWith(eventHandler);

		RingBuffer<RecordLogEvent> ringBuffer = disruptor.start();
		
		recordLogEventProducer = new RecordLogEventProducer(ringBuffer);
		
		int cols = context.getTable().getColumnSize();

		dispatcher.dispatch(rFirst);
		
		rSize = rSize / 2;
		
		for (int i = 0; i < rSize; i++) {
			recordLogEventProducer.publish(RecordLog.newRecordLog(cols));
		}

		countDownLatch.await();
		
		dispatcher.readRecordOver();
		
		//FIXME 判断重放已经完成
		
		logger.info("读取并分发所有记录耗时 : {} .", System.currentTimeMillis() - start);
	}

	/**
	 * @return the recordLogEventProducer
	 */
	public RecordLogEventProducer getRecordLogEventProducer() {
		return recordLogEventProducer;
	}

}
