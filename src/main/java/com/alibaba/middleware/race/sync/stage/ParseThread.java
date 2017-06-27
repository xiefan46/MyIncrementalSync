package com.alibaba.middleware.race.sync.stage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.sync.Config;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.BufferPool;
import com.alibaba.middleware.race.sync.RangeSearcher;
import com.alibaba.middleware.race.sync.model.result.ReadResult;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.result.ParseResult;
import com.alibaba.middleware.race.sync.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiefan on 6/25/17.
 */
public class ParseThread implements Runnable, Constants {

	private ConcurrentLinkedQueue<ReadResult>	readResults;

	private Table							table		= Context.getInstance().getTable();

	private ByteBufReader					byteBufReader	= new ByteBufReader();

	private ByteBuffer[]					partitions	= new ByteBuffer[Config.CALCULATOR_COUNT];

	private ParseResult[]					parseResults	= new ParseResult[Config.CALCULATOR_COUNT];

	private RangeSearcher					rangeSearcher	= Context.getInstance()
			.getRangeSearcher();

	private CalculateStage					calculateStage;

	private RecordLog						recordLog;

	private static final Logger				logger		= LoggerFactory
			.getLogger(ParseThread.class);

	private AtomicInteger					count		= new AtomicInteger(0);

	private volatile boolean					stop			= false;

	private ParseStage						parseStage;

	private Context						context		= Context.getInstance();

	private BufferPool						recordPool;

	public ParseThread(CalculateStage calculateStage, ParseStage parseStage) {

		this.calculateStage = calculateStage;

		this.readResults = parseStage.getReadResultQueue();

		partitions = new ByteBuffer[Config.CALCULATOR_COUNT];

		parseResults = new ParseResult[Config.CALCULATOR_COUNT];

		this.recordPool = new BufferPool(Config.RECORD_BUFFER_COUNT, Config.RECORD_BUFFER_SIZE);

		int cols = table.getColumnSize();

		this.recordLog = RecordLog.newRecordLog(cols);

		this.parseStage = parseStage;

	}

	@Override
	public void run() {
		try {
			while (!stop || !readResults.isEmpty()) {
				while (!readResults.isEmpty()) {
					ReadResult readResult = readResults.poll();
					if (readResult != null) {
						if (readResult.getId() == -1) {
							parseStage.notifyStop();
							break;
						}
						dealResult(readResult);
						Context.getInstance().getBlockBufferPool()
								.free(readResult.getBuffer());
					} else {
						Thread.currentThread().sleep(10);
					}
				}
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void dealResult(ReadResult result) throws IOException {
		ByteBuffer buffer = result.getBuffer();
		int id = result.getId();
		if (id == -1 || buffer == null) {
			return;
		}
		reset(result);
		byte[] tableSchema = table.getTableSchemaBytes();

		while (buffer.hasRemaining()) {
			recordLog.reset();
			boolean read = byteBufReader.read(table, buffer, tableSchema, recordLog);
			/*
			 * if (count.incrementAndGet() % 5000000 == 0) {
			 * logger.info("deal count : {}", count.get()); }
			 */
			/*
			 * logger.info("get record. Alter type : {}. Pk : {} OldPk : {}",
			 * (char) recordLog.getAlterType(), recordLog.getPk(),
			 * recordLog.getBeforePk());
			 */
			if (read && filter(recordLog)) {
				dealRecordLog(recordLog);
			}
		}

		//发送解析结果
		finish();

	}

	private boolean filter(RecordLog recordLog) {
		if (recordLog.getAlterType() == PK_UPDATE) {
			return inRange(recordLog.getBeforePk());
		} else {
			return inRange(recordLog.getPk());
		}
	}

	private void dealRecordLog(RecordLog recordLog) {
		if (recordLog.getAlterType() == Constants.PK_UPDATE) {
			if (!inRange(recordLog.getBeforePk()))
				return;
		} else {
			if (!inRange(recordLog.getPk()))
				return;
		}
		ByteBuffer buffer;
		switch (recordLog.getAlterType()) {
		case INSERT:
			//buffer = getBufferByPk(recordLog.getPk());
			buffer = getBufferWithRemaining(recordLog.getPk(), 50);
			buffer.put(INSERT);
			buffer.putInt(recordLog.getPk());
			buffer.put(recordLog.getColumns());
			break;
		case UPDATE:
			//buffer = getBufferByPk(recordLog.getPk());
			buffer = getBufferWithRemaining(recordLog.getPk(), 50);
			for (int i = 0; i < recordLog.getEdit(); i++) { //为了方便,所有字段的更新分开成不同record
				buffer.put(UPDATE);
				buffer.putInt(recordLog.getPk());
				buffer.put(recordLog.getColumns(), i * 8, 8);
			}
			break;
		case DELETE:
			//buffer = getBufferByPk(recordLog.getPk());
			buffer = getBufferWithRemaining(recordLog.getPk(), 10);
			buffer.put(DELETE);
			buffer.putInt(recordLog.getPk());
			break;
		case PK_UPDATE:
			ByteBuffer oldBuffer = getBufferWithRemaining(recordLog.getBeforePk(), 10);
			oldBuffer.put(PK_UPDATE);
			oldBuffer.putInt(recordLog.getBeforePk());
			break;
		default:
			throw new RuntimeException(
					"Can not handle alter type. Type : " + (char) recordLog.getAlterType());
		}
	}

	private ByteBuffer getBufferByPk(int pk) {
		int threadId = rangeSearcher.searchForDealThread(pk);
		ByteBuffer buffer = partitions[threadId];
		return buffer;
	}

	private ByteBuffer getBufferWithRemaining(int pk, int remaining) {
		int threadId = rangeSearcher.searchForDealThread(pk);
		ByteBuffer ret = partitions[threadId];
		if (ret.remaining() < remaining) {
			ret.flip();
			parseResults[threadId].addBuffer(ret);
			ret = recordPool.allocate();
			ret.clear();
			partitions[threadId] = ret;
		}
		return ret;
	}

	private boolean inRange(int pk) {
		if (pk > Context.getInstance().getStartPk() && pk < Context.getInstance().getEndPk())
			return true;
		return false;
	}

	private void reset(ReadResult result) {
		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = recordPool.allocate();
			partitions[i].clear();
			parseResults[i] = new ParseResult(result.getId(), this.recordPool);
		}

	}

	private void finish() {

		for (int i = 0; i < partitions.length; i++) {
			partitions[i].flip();
			parseResults[i].addBuffer(partitions[i]);
			calculateStage.submit(i, parseResults[i]);
			partitions[i] = null;
			parseResults[i] = null;
		}

	}

	class ByteBufReader {

		private int			startId	= Context.getInstance().getStartPk();

		private int			endId	= Context.getInstance().getEndPk();

		private RecordLogCodec2	codec	= new RecordLogCodec2();

		public boolean read(Table table, ByteBuffer buf, byte[] tableSchema, RecordLog r)
				throws IOException {
			int oldPos = buf.position();
			logger.info("buffer. limit : {}, position : {},capacity : {}",
					buf.limit(), buf.position(), buf.capacity());
			System.out.println("last char : n ? " + (buf.get(buf.limit() - 1) == '\n'));
			buf.position(oldPos);
			byte[] readBuffer = buf.array();
			int offset = buf.position();
			if (!buf.hasRemaining()) {
				return false;
			}
			int off = codec.decode(table, readBuffer, tableSchema, offset, r, startId, endId);
			buf.position(off + 1);
			return r.isRead();
		}

	}

	public void stop() {
		this.stop = true;
	}
}
