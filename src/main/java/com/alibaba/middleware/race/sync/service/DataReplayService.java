package com.alibaba.middleware.race.sync.service;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.sync.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.map.ArrayMap;
import com.alibaba.middleware.race.sync.map.NormalMap;
import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.entity.ReplayTask;
import com.alibaba.middleware.race.sync.entity.SendTask;
import com.alibaba.middleware.race.sync.metrics.ReplayMetrics;
import com.alibaba.middleware.race.sync.util.Timer;

/**
 * Created by xiefan on 6/24/17.
 */
public class DataReplayService implements Constants {
	private static Logger				stat		= LoggerFactory.getLogger("stat");
	private Replayer[]					replayers;
	private Context					context	= Context.getInstance();
	private ConcurrentLinkedQueue<SendTask>	output;
	private int						replayerCount;
	private boolean					inRange;

	public DataReplayService(ConcurrentLinkedQueue<SendTask> output, boolean inRange,
			int replayerCount) {
		this.output = output;
		this.replayerCount = replayerCount;
		this.inRange = inRange;
	}

	public void addTask(int partitionId, ReplayTask task) {
		replayers[partitionId].addTask(task);
	}

	public void start() {
		final long start = System.currentTimeMillis();
		replayers = new Replayer[replayerCount];
		final CountDownLatch latch = new CountDownLatch(replayers.length);
		for (int i = 0; i < replayers.length; ++i) {
			replayers[i] = new Replayer(latch, replayers, inRange, i);
		}
		for (int i = 0; i < replayers.length; ++i) {
			new Thread(replayers[i]).start();
		}

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					latch.await();
					long end = System.currentTimeMillis();

					ReplayMetrics replayMetrics = new ReplayMetrics();
					for (int i = 0; i < replayers.length; ++i)
						replayMetrics.merge(replayers[i].getReplayMetrics());
					if (inRange)
						output.offer(SendTask.END_TASK);
					stat.info("replay finish, cost {} ms, {}", end - start,
							replayMetrics.desc());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	class Replayer implements Runnable {
		private ConcurrentLinkedQueue<ReplayTask>	input		= new ConcurrentLinkedQueue<>();
		private Map<Long, ReplayTask>				taskMap		= new HashMap<>();
		private CountDownLatch					latch;
		private long							now			= 0;
		private List<Column>					columnList	= context.getColumnList();
		private Replayer[]						replayers;
		private Map<Long, Long>					waitMap		= new ConcurrentHashMap<>();
		private ReplayMetrics					replayMetrics	= new ReplayMetrics();

		private boolean						inRange;
		private IReplayMap						replayMap;
		private int							partitionId;

		public Replayer(CountDownLatch latch, Replayer[] replayers, boolean inRange,
				int partitionId) {
			this.latch = latch;
			this.replayers = replayers;
			this.inRange = inRange;
			this.partitionId = partitionId;
			if (inRange) {
				long start = context.getRangePartitioner().getStart(partitionId);
				long end = context.getRangePartitioner().getEnd(partitionId);
				replayMap = new ArrayMap((int) start, (int) end);
			} else {
				replayMap = new NormalMap();
			}
		}

		public void addTask(ReplayTask task) {
			input.offer(task);
		}

		public void addWait(long waitId, long offset) {
			waitMap.put(waitId, offset);
		}

		private void writeData(Column column, ByteBuffer buffer, int offset) {
			if (column.isInt()) {
				long val = buffer.getLong();
				replayMap.update(offset, column.getId() - 1, val);
			} else {
				short length = buffer.getShort();
				replayMap.update(offset, column.getId() - 1, buffer.array(), buffer.position(),
						length);
				buffer.position(buffer.position() + length);
			}
		}

		private void replayInsert(ByteBuffer buffer) {
			// 因为是单线程replay,所以一定可以保证在insert的时候,offsetmap里是没有这个主键的,否则是冲突的,因此下面可能是多余的
			long pk = buffer.getLong();
			int offset = replayMap.addPk(pk);

			for (int i = 1; i < columnList.size(); ++i) {
				Column column = columnList.get(i);
				writeData(column, buffer, offset);
			}
		}

		private long updateColumns(ByteBuffer buffer, long pk) {
			int offset = replayMap.getOffset(pk);
			int count = buffer.get(); // 列数
			for (int i = 0; i < count; ++i) {
				int columnId = buffer.get();
				Column column = columnList.get(columnId);
				writeData(column, buffer, offset);
			}
			return offset;
		}

		private void replayNormalUpdate(ByteBuffer buffer) {
			// 此时必然是存在这个对象的
			long pk = buffer.getLong();
			updateColumns(buffer, pk);
		}

		private void replayUpdatePk(ByteBuffer buffer) {
			long oldPk = buffer.getLong();
			long newPk = buffer.getLong();
			long waitId = buffer.getLong();
			long offset = updateColumns(buffer, oldPk);

			replayMap.remove(oldPk);

			//然后,要将这个block传递给newPk对应的replay,并告诉他waitid是多少
			//            int partitionId = getPartitionId(newPk);
			//            replayers[partitionId].addWait(waitId, offset);
		}

		private void replayWait(ByteBuffer buffer) {
			long pk = buffer.getLong();
			long waitId = buffer.getLong();
			//            Long offset;
			//            while ((offset = waitMap.get(waitId)) == null) {
			//                Timer.sleep(1, 0);
			//            }
			//            waitMap.remove(waitId);
			//            replayMap.copy(pk, offset);
		}

		// 回放所有操作
		private void replayOp(ByteBuffer buffer) {
			while (buffer.hasRemaining()) {
				byte op = buffer.get();
				if (op == UPDATE) { //普通更新操作
					replayNormalUpdate(buffer);
				} else if (op == INSERT) {
					replayInsert(buffer);
				} else if (op == UPDATE_PK) {
					replayUpdatePk(buffer);
				} else if (op == DELETE) {
					long pk = buffer.getLong();
					replayMap.remove(pk);
				} else {
					replayWait(buffer);
				}
			}
		}

		private void work() {
			while (taskMap.containsKey(now)) {
				ReplayTask task = taskMap.get(now);
				taskMap.remove(now);
				now++;
				for (ByteBuffer buffer : task.getList()) {
					replayMetrics.totalSize += buffer.limit();
					if (inRange)
						replayOp(buffer);
					task.getPool().freeBuffer(buffer);
				}
			}
		}

		@Override
		public void run() {

			//            if (inRange) {
			//                long start = context.getRangePartitioner().getStart(partitionId);
			//                long end = context.getRangePartitioner().getEnd(partitionId);
			//                replayMap = new ArrayMap((int)start, (int)end);
			//            }
			//            else {
			//                replayMap = new NormalMap();
			//            }

			while (true) {
				ReplayTask task = input.poll();
				if (task == null) {
					Timer.sleep(1, 0);
					continue;
				}
				if (task.isEnd()) {
					work();
					break;
				}
				taskMap.put(task.getEpoch(), task);
				work();
			}

			if (inRange)
				output.offer(new SendTask(replayMap, partitionId));
			latch.countDown();
		}

		public ReplayMetrics getReplayMetrics() {
			return replayMetrics;
		}
	}
}
