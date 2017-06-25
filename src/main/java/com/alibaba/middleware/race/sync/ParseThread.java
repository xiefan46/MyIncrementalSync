package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;

public class ParseThread extends Thread {

	private static Logger	logger	= LoggerFactory.getLogger(ParseThread.class);

	private ByteBuf		buf;

	private Object			lock		= new Object();

	private volatile boolean	isRunning	= true;

	private boolean		work;

	private RecordLog		root;
	
	private int			limit;

	private ReaderThread	readerThread;
	
	private ByteBufReader	byteBufReader = new ByteBufReader();
	
	private Dispatcher		dispatcher;

	private int			index;

	public ParseThread(ReaderThread readerThread, ByteBuf buf, int index) {
		super("parse-" + index);
		this.buf = buf;
		this.index = index;
		this.readerThread = readerThread;
		this.dispatcher = readerThread.getContext().getDispatcher();
	}

	@Override
	public void run() {
		for (;;) {
			if (!work) {
				wait4Work();
			}
			if (!isRunning) {
				break;
			}
			try {
				work();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	private void work() throws Exception {
		ReaderThread readerThread = this.readerThread;
		ByteBuf buf = this.buf;
		Context context = readerThread.getContext();
		Table table = context.getTable();
		byte[] tableSchema = table.getTableSchemaBytes();
		int cols = table.getColumnSize();
		ByteBufReader reader = this.byteBufReader;
		if (!buf.hasRemaining()) {
			return;
		}
		if (root == null) {
			root = new RecordLog();
			root.newColumns(cols);
		}
		limit = 1;
		RecordLog cr = root;
		cr.reset();
		reader.read(table, buf, tableSchema, cr);
		for (; buf.hasRemaining();) {
			cr = getNext(cr, cols);
			cr.reset();
			reader.read(table, buf, tableSchema, cr);
		}
		work = false;
		readerThread.done(index);
	}
	
	private RecordLog getNext(RecordLog recordLog,int cols){
		limit++;
		RecordLog next = recordLog.getNext();
		if (next == null) {
			next = new RecordLog();
			next.newColumns(cols);
			recordLog.setNext(next);
			return next;
		}
		return next;
	}
	
	public void dispatch() throws InterruptedException{
		int limit = this.limit;
		Dispatcher dispatcher = this.dispatcher;
		RecordLog r = this.root;
		for (int i = 0; i < limit; i++) {
			dispatcher.dispatch(r);
			r = r.getNext();
		}
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
		synchronized (lock) {
			lock.notify();
		}
	}

	private void wait4Work() {
		synchronized (lock) {
			try {
				lock.wait();
			} catch (Throwable e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	public int getLimit() {
		return limit;
	}
}
