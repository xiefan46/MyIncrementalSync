package com.alibaba.middleware.race.sync.service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Config;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.common.Partitioner;
import com.alibaba.middleware.race.sync.entity.ParseTask;
import com.alibaba.middleware.race.sync.entity.ReplayTask;
import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.util.Timer;

/**
 * Created by xiefan on 6/24/17.
 */
public class DataParseService implements Constants {

	private static final Logger				logger		= LoggerFactory
			.getLogger(DataParseService.class);

	public static final int					PARSER_NUM	= 12;

	private static Logger					stat			= LoggerFactory.getLogger("stat");

	private Context						context		= Context.getInstance();

	private ConcurrentLinkedQueue<ParseTask>	input;

	private List<byte[]>					columnByteList	= context.getColumnByteList();

	private DataReplayService[]				replayServices	= new DataReplayService[2];

	public DataParseService(ConcurrentLinkedQueue<ParseTask> input,
			DataReplayService inRangeReplayService, DataReplayService outRangeReplayService) {
		this.input = input;
		replayServices[0] = inRangeReplayService;
		replayServices[1] = outRangeReplayService;
	}

	public void start() {
		final long start = System.currentTimeMillis();
		final Parser[] parsers = new Parser[PARSER_NUM];
		final CountDownLatch latch = new CountDownLatch(parsers.length);
		for (int i = 0; i < parsers.length; ++i) {
			parsers[i] = new Parser(latch);
			new Thread(parsers[i]).start();
		}
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					latch.await();

					for (int i = 0; i < Config.INRANGE_REPLAYER_COUNT; ++i)
						replayServices[0].addTask(i, ReplayTask.END_TASK);
					for (int i = 0; i < Config.OUTRANGE_REPLAYER_COUNT; ++i)
						replayServices[1].addTask(i, ReplayTask.END_TASK);
					long end = System.currentTimeMillis();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	class Parser implements Runnable {
		private CountDownLatch	latch;
		private Context		context		= Context.getInstance();
		private BufferPool		readBufferPool	= context.getReadBufferPool();
		private List<Column>	columnList	= context.getColumnList();
		private int			columnCount	= columnList.size() - 1;

		private long			startPk		= context.getStartPk();
		private long			endPk		= context.getEndPk();

		private BufferPool[]	pools		= new BufferPool[2];

		private ByteBuffer[][]	partitions	= new ByteBuffer[2][];
		private ReplayTask[][]	replayTasks	= new ReplayTask[2][];

		private Partitioner[]	partitioners	= new Partitioner[2];

		private byte[]			buf;

		public Parser(CountDownLatch latch) {
			this.latch = latch;

			partitions[0] = new ByteBuffer[Config.INRANGE_REPLAYER_COUNT];
			partitions[1] = new ByteBuffer[Config.OUTRANGE_REPLAYER_COUNT];

			replayTasks[0] = new ReplayTask[Config.INRANGE_REPLAYER_COUNT];
			replayTasks[1] = new ReplayTask[Config.OUTRANGE_REPLAYER_COUNT];

			partitioners[0] = context.getRangePartitioner();
			partitioners[1] = context.getHashPartitioner();

		}

		private int pos = 0;

		//此时pos处于long的最高位,函数返回时,pos跳过了long之后的'|'
		private long getLong() {
			long ret = 0;
			while (buf[pos] != '|') {
				ret = ret * 10 + buf[pos++] - '0';
			}
			pos++; // 跳过'|'
			return ret;
		}

		//此时pos的位置一定不能是'|'
		private void walkThroughNextSeperator() {
			while (buf[pos] != '|')
				pos++;
			pos++;
		}

		// pos处于列名首字符,返回后,pos跳过了列名的'|'
		private Column getColumn() {
			Column ret = null;
			for (int i = pos; i < buf.length; ++i) {
				if (buf[i] == ':') {
					for (int j = 1; j < columnByteList.size(); ++j)
						if (columnByteList.get(j).length == i - pos) {
							ret = columnList.get(j);
						}
					pos = i + 1;
					walkThroughNextSeperator();
					return ret;
				}
			}
			return null;
		}

		// 此时pos必须在val的首字符上,返回后,pos在下一个列的首字符,或者结尾的'\n'处
		private void putColumnData(Column column, ByteBuffer partition) {
			if (column.isInt()) {
				partition.putLong(getLong());
			} else {
				// 这里假设所有的字符串长度都不超过6byte
				int tmp = pos;
				while (buf[pos] != '|')
					pos++;
				partition.putShort((short) (pos - tmp));
				partition.put(buf, tmp, pos - tmp);
				pos++;
			}
		}

		// flag 0 表示inrange, remaining表示该partiton需要至少剩余那么多的字节数
		private ByteBuffer getBufferWithRemaining(int flag, int remaining, int partitionId) {
			ByteBuffer ret = partitions[flag][partitionId];
			if (ret.remaining() < remaining) {
				ret.flip();
				replayTasks[flag][partitionId].addBuffer(ret);
				ret = getBuffer(pools[flag]);
				partitions[flag][partitionId] = ret;
			}
			return ret;
		}

		// 区间内返回0,否则1
		private int getFlag(long pk) {
			return (pk > startPk && pk < endPk) ? 0 : 1;
		}

		// insert 操作 parse,进入函数时,pos的位置为id首字符
		private void insert() {
			walkThroughNextSeperator();
			walkThroughNextSeperator();
			long pk = getLong();
			int flag = getFlag(pk);
			int partitionId = partitioners[flag].getPartitionId(pk);
			ByteBuffer partition = getBufferWithRemaining(flag, 1 + 8 + columnCount * 8,
					partitionId);
			partition.put(INSERT);
			partition.putLong(pk);
			for (int i = 1; i < columnList.size(); ++i) {
				// 每轮循环开始的时候,pos处一定是列名首字符
				Column column = columnList.get(i);
				walkThroughNextSeperator();
				walkThroughNextSeperator();
				//此时pos处一定是val的第一个byte
				putColumnData(column, partition);
			}
			pos++; // 此处是跳过本条记录的'\n',到达了下一条记录的开始
		}

		// pos为列名首字符
		private void putUpdateColumn(ByteBuffer partition) {
			int tmp = partition.position();
			partition.put((byte) 0); // 占位符,最终是要更新为列数的
			int count = 0;
			while (true) {
				if (buf[pos] == '\n' || pos >= buf.length)
					break;
				count++;
				Column column = getColumn();
				walkThroughNextSeperator();
				partition.put((byte) column.getId());
				putColumnData(column, partition);
			}
			partition.put(tmp, (byte) count);
			pos++;
		}

		// 普通update,进入函数时,pos位置为列名首字符
		private void normalUpdate(long pk) {
			int flag = getFlag(pk);
			int partitionId = partitioners[flag].getPartitionId(pk);
			ByteBuffer partition = getBufferWithRemaining(flag, 1 + 8 + columnCount * (1 + 8),
					partitionId);
			partition.put(UPDATE);
			partition.putLong(pk);
			putUpdateColumn(partition);
		}

		private int waitCounter = 0;

		//主键更新,进入函数时,pos位置为列名首字符
		private void updatePk(long oldPk, long newPk, long epoch) {
			//这里需要向两个分区写入数据,oldpk所在分区就是正常的updatepk操作,而在newpk所在分区则要加入一个标志位,表示该主键由别的主键更新而来

			//在newpk所在partition加入等待标志位
			int flag = getFlag(newPk);
			int partitionId = partitioners[flag].getPartitionId(newPk);
			ByteBuffer partition = getBufferWithRemaining(flag, 1 + 8 * 2, partitionId);
			partition.put(WAIT);
			partition.putLong(newPk);
			long waitId = epoch << 32 | (waitCounter++);
			partition.putLong(waitId);

			// 在oldpk所在partition加入更新操作
			flag = getFlag(oldPk);
			partitionId = partitioners[flag].getPartitionId(oldPk);
			partition = getBufferWithRemaining(flag, 1 + 8 * 3 + 9 * columnCount + 1,
					partitionId);
			partition.put(UPDATE_PK).putLong(oldPk).putLong(newPk).putLong(waitId);
			putUpdateColumn(partition);
		}

		private void delete() {
			walkThroughNextSeperator();
			long pk = getLong();
			walkThroughNextSeperator();
			int flag = getFlag(pk);
			int partitionId = partitioners[flag].getPartitionId(pk);
			ByteBuffer partition = getBufferWithRemaining(flag, 1 + 8, partitionId);
			partition.put(DELETE).putLong(pk);
			while (pos < buf.length && buf[pos] != '\n')
				walkThroughNextSeperator();
			pos++;
		}

		private void parse(ByteBuffer buffer, long epoch) {
			waitCounter = 0;
			for (int i = 0; i < 2; ++i) {
				for (int j = 0; j < partitions[i].length; ++j) {
					partitions[i][j] = getBuffer(pools[i]);
					replayTasks[i][j] = new ReplayTask(pools[i], epoch);
				}
			}
			pos = 0;
			buf = buffer.array();
			while (pos < buffer.limit()) {

				int tmp = pos;

				// 找到OP开始的位置
				pos += 15;
				walkThroughNextSeperator();
				pos += 34;

				//                if (buf[pos] == '|') pos++;
				//                for (int i = 0; i < 4; ++i)
				//                walkThroughNextSeperator();
				byte op = buf[pos];
				pos += 2;
				// 此时pos必定在id的开始处
				if (op == 'I') {
					insert();
				} else if (op == 'U') {
					walkThroughNextSeperator(); // 到达oldpk的开始

					long oldPk = getLong();
					long newPk = getLong();
					//此时pos处为列名首字符
					if (oldPk == newPk) {
						// 主键不变更
						normalUpdate(oldPk);
					} else {
						updatePk(oldPk, newPk, epoch);
					}
				} else if (op == 'D') {
					delete();
				} else {
					logger.info("encounter error when parsing record : "
							+ new String(buf, tmp, pos - tmp - 1));
					System.exit(1);
				}
			}

			// 将parse完得到的replay信息加入到对应的replay队列

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

		private ByteBuffer getBuffer(BufferPool pool) {
			ByteBuffer ret;
			while ((ret = pool.getBuffer()) == null) {

			}
			return ret;
		}

		private long debug = 0;

		//        private ByteBuffer localBuffer = ByteBuffer.allocate(Config.READ_BUFFER_SIZE);

		@Override
		public void run() {
			pools[0] = new BufferPool(Config.INRANGE_PARTITION_BUFFER_COUNT,
					Config.INRANGE_PARTITION_BUFFER_SIZE);
			pools[1] = new BufferPool(Config.OUTRANGE_PARTITION_BUFFER_COUNT,
					Config.OUTRANGE_PARTITION_BUFFER_SIZE);
			ByteBuffer localBuffer = ByteBuffer.allocate(Config.READ_BUFFER_SIZE);
			while (true) {
				ParseTask task = input.poll();
				if (task == null) {
					Timer.sleep(1, 0);
					continue;
				}
				if (task.isEnd()) {
					input.offer(task);
					break;
				}
				ByteBuffer buffer = task.getBuffer();
				localBuffer.clear();
				localBuffer.put(buffer);
				localBuffer.flip();
				readBufferPool.freeBuffer(buffer);
				debug = task.getEpoch();
				parse(localBuffer, task.getEpoch());

			}
			latch.countDown();
		}

	}
}
