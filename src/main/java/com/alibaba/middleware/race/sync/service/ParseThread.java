package com.alibaba.middleware.race.sync.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.race.sync.Config;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.common.Partitioner;
import com.alibaba.middleware.race.sync.model.Block;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.ReplayTask;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * Created by xiefan on 6/25/17.
 */
public class ParseThread implements Runnable, Constants {

	private ConcurrentLinkedQueue<Block>	blocksQueue;

	private boolean				isStop			= false;

	private Table					table			= Context.getInstance().getTable();

	private ByteBufReader			byteBufReader		= new ByteBufReader();

	private ByteBuffer[][]			partitions		= new ByteBuffer[2][];

	private ReplayTask[][]			replayTasks		= new ReplayTask[2][];

	private Partitioner[]			partitioners		= new Partitioner[2];

	private BufferPool				recordLogBufferPool	= Context.getInstance()
			.getRecordLogPool();

	private DataReplayService[]		replayServices		= new DataReplayService[2];

	private RecordLog				recordLog;

	public ParseThread(ConcurrentLinkedQueue<Block> blocksQueue,
			DataReplayService inRangeReplayService, DataReplayService outRangeReplayService) {
		this.blocksQueue = blocksQueue;

		replayServices[0] = inRangeReplayService;

		replayServices[1] = outRangeReplayService;

		partitions[0] = new ByteBuffer[Config.INRANGE_REPLAYER_COUNT];

		partitions[1] = new ByteBuffer[Config.OUTRANGE_REPLAYER_COUNT];

		replayTasks[0] = new ReplayTask[Config.INRANGE_REPLAYER_COUNT];

		replayTasks[1] = new ReplayTask[Config.OUTRANGE_REPLAYER_COUNT];

		partitioners[0] = Context.getInstance().getRangePartitioner();

		partitioners[1] = Context.getInstance().getHashPartitioner();

		int cols = table.getColumnSize();

		this.recordLog = RecordLog.newRecordLog(cols);

	}

	@Override
	public void run() {
		try {
			while (!blocksQueue.isEmpty() || !isStop) {
				while (!blocksQueue.isEmpty()) {
					Block block = blocksQueue.poll();
					if (block != null) {
						dealBlock(block);
						Context.getInstance().getBlockBufferPool()
								.freeBuffer(block.getBuffer());
					} else {
						Thread.currentThread().sleep(10);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void dealBlock(Block block) throws IOException {
		ByteBuffer buffer = block.getBuffer();
		int id = block.getBlockId();
		if (id == -1 || buffer == null || !buffer.hasRemaining()) {
			return;
		}
		reset(block);
		byte[] tableSchema = table.getTableSchemaBytes();

		while (buffer.hasRemaining()) {
			recordLog.reset();
			byteBufReader.read(table, buffer, tableSchema, recordLog);
			dealRecordLog(recordLog);
		}
		//发送解析结果
		finish();

	}

	private void dealRecordLog(RecordLog recordLog) {
		ByteBuffer buffer = getBufferByPk(recordLog.getPk());
		switch (recordLog.getAlterType()) {
		case INSERT:
			buffer.put(INSERT);
			buffer.putInt(recordLog.getPk());
			buffer.put(recordLog.getColumns());
			break;
		case UPDATE:
			for (int i = 0; i < recordLog.getEdit(); i++) { //为了方便,所有字段的更新分开成不同record
				buffer.put(UPDATE);
				buffer.putInt(recordLog.getPk());
				buffer.put(recordLog.getColumns(), i * 8, 8);
			}
			break;
		case DELETE:
			buffer.put(DELETE);
			buffer.putInt(recordLog.getPk());
			break;
		case PK_UPDATE:
			ByteBuffer oldBuffer = getBufferByPk(recordLog.getBeforePk());
			oldBuffer.put(PK_UPDATE);
			oldBuffer.putInt(recordLog.getPk());
			oldBuffer.putInt(recordLog.getBeforePk());
			break;
		default:
			throw new RuntimeException(
					"Can not handle alter type. Type : " + (char) recordLog.getAlterType());
		}
	}

	private ByteBuffer getBufferByPk(int pk) {
		boolean inRange = inRange(recordLog.getPk());
		Partitioner partitioner = inRange ? partitioners[0] : partitioners[1];
		ByteBuffer[] buffers = inRange ? partitions[0] : partitions[1];
		int partId = partitioner.getPartitionId(recordLog.getPk());
		ByteBuffer buffer = buffers[partId];
		return buffer;
	}

	private boolean inRange(int pk) {
		if (pk > Context.getInstance().getStartPk() && pk < Context.getInstance().getEndPk())
			return true;
		return false;
	}

	private void reset(Block block) {
		for (int i = 0; i < 2; ++i) {
			for (int j = 0; j < partitions[i].length; ++j) {
				partitions[i][j] = recordLogBufferPool.getBufferWait();
				replayTasks[i][j] = new ReplayTask(null, block.getBlockId());
			}
		}
	}

	private void finish() {
		for (int i = 0; i < 2; ++i) {
			for (int j = 0; j < partitions[i].length; ++j) {
				partitions[i][j].flip();
				replayTasks[i][j].addBuffer(partitions[i][j]);
				replayServices[i].addTask(j, replayTasks[i][j]);
				partitions[i][j] = null;
				replayTasks[i][j] = null;
			}
		}
	}

	public void stop() {
		this.isStop = true;
	}

	class ByteBufReader {

		private RecordLogCodec2 codec = new RecordLogCodec2();

		public boolean read(Table table, ByteBuffer buf, byte[] tableSchema, RecordLog r)
				throws IOException {
			byte[] readBuffer = buf.array();
			int offset = buf.position();
			if (!buf.hasRemaining()) {
				return false;
			}
			int off = codec.decode(table, readBuffer, tableSchema, offset, r);
			buf.position(off + 1);
			return true;
		}

	}
}
