package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;

public class RecalculateThread extends Thread {

	private static Logger		logger	= LoggerFactory.getLogger(RecalculateThread.class);

	private ByteBuf			buf;

	private ReentrantLock		lock		= new ReentrantLock();

	private volatile boolean	isRunning	= true;

	private Condition			waiter	= lock.newCondition();

	private boolean			work;

	private ReaderThread		readerThread;

	private int				index;

	private Map<Integer, long[]>	records;

	public Map<Integer, long[]> getRecords() {
		return records;
	}

	public long[] getRecord(int id) {
		return records.get(id);
	}

	public RecalculateThread(ReaderThread readerThread, ByteBuf buf, Map<Integer, long[]> records,
			int index) {
		super("recal-" + index);
		this.buf = buf;
		this.index = index;
		this.readerThread = readerThread;
		this.records = records;
	}

	@Override
	public void run() {

		RecordLog r = RecordLog
				.newRecordLog(readerThread.getContext().getTable().getColumnSize());

		for (;;) {
			Condition waiter = this.waiter;

			if (!work) {
				ReentrantLock lock = this.lock;
				lock.lock();
				try {
					waiter.await();
				} catch (Throwable e) {
					logger.error(e.getMessage(), e);
				}finally{
					lock.unlock();
				}
			}

			if (!isRunning) {
				break;
			}

			try {
				work(r);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

	}

	private void work(RecordLog r) throws Exception {
		ReaderThread readerThread = this.readerThread;
		ByteBuf buf = this.buf;
		Map<Integer, long[]> records = this.records;
		Context context = readerThread.getContext();
		Table table = context.getTable();
		RecordLogReceiver receiver = context.getReceiver();
		byte[] tableSchema = table.getTableSchemaBytes();
		ByteBufReader reader = ByteBufReader.get();
		for (; buf.hasRemaining();) {
			r.reset();
			if (!reader.read(table, buf, tableSchema, r)) {
				continue;
			}
//			receiver.received(table, records, r);
		}
		work = false;
		readerThread.done(index);
	}

	public ReentrantLock getLock() {
		return lock;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public boolean isWork() {
		return work;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	public void setWork(boolean work) {
		this.work = work;
	}

	public ByteBuf getBuf() {
		return buf;
	}

	public void setBuf(ByteBuf buf) {
		this.buf = buf;
	}

	public void wakeup() {
		ReentrantLock lock = this.lock;
		lock.lock();
		try {
			waiter.signal();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}finally{
			lock.unlock();
		}
	}

}
