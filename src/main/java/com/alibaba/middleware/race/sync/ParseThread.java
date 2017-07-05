package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.MyList;
import com.generallycloud.baseio.buffer.ByteBuf;

public class ParseThread extends WorkThread {

	private Logger					logger		= LoggerFactory.getLogger(getClass());

	private MyList<RecordLog>		rootRecord1;

	private MyList<RecordLog>		rootRecord2;

	private boolean				use1;

	private MyList<RecordLog>		result;

	private volatile boolean		done;

	private boolean				workDone;

	private BlockingQueue<ByteBuf>	bufs;

	private int					limit;

	private Context				context;

	private ByteBufReader			byteBufReader	= new ByteBufReader();

	public ParseThread(Context context, int index, int recordSize) {
		super("parse-", index);
		this.context = context;
		this.setWork(true);
		this.bufs = new ArrayBlockingQueue<>(context.getParseThreadNum() * 2);
		this.initRecordList(recordSize, context.getTable().getColumnSize());
	}

	private void initRecordList(int recordSize, int cols) {
		this.rootRecord1 = new MyList<>(recordSize);
		this.rootRecord2 = new MyList<>(recordSize);
		for (int i = 0; i < recordSize; i++) {
			rootRecord1.add(RecordLog.newRecordLog(cols));
			rootRecord2.add(RecordLog.newRecordLog(cols));
		}
	}

	protected void work() throws Exception {
		BlockingQueue<ByteBuf> bufs = this.bufs;
		ByteBuf buf = bufs.poll(16, TimeUnit.MICROSECONDS);
		if (buf == null) {
			return;
		}
		if (done) {
			wait4Work();
		}
		Table table = context.getTable();
		byte[] tableSchema = table.getTableSchemaBytes();
		ByteBufReader reader = this.byteBufReader;
		if (!buf.hasRemaining()) {
			this.limit = 0;
			this.done = true;
			this.workDone = true;
			this.context.getMainThread().setWorkDone();
			return;
		}
		MyList<RecordLog> result;
		MyList<RecordLog> cr;
		if (use1) {
			use1 = false;
			result = rootRecord1;
			cr = rootRecord1;
		} else {
			use1 = true;
			result = rootRecord2;
			cr = rootRecord2;
		}
		int startId = context.getStartId();
		int endId = context.getEndId();

		cr.clear();
		RecordLog crv = cr.get();
		crv.reset();
		for (; buf.hasRemaining();) {
			if (reader.read(table, buf, tableSchema, crv, startId, endId)) {
				crv = cr.get();
				crv.reset();
			}
		}
		context.getByteBufPool().free(buf);
		this.result = result;
		this.done = true;
	}

	public int getLimit() {
		return limit;
	}

	/**
	 * @return the rootRecord
	 */
	public MyList<RecordLog> getResult() {
		return result;
	}

	public void offerBuf(ByteBuf buf) {
		bufs.offer(buf);
	}

	public boolean isDone() {
		return done || workDone;
	}

	@Override
	public void startWork() {
		done = false;
		limit = 0;
		super.startWork();
	}

	@Override
	Logger getLogger() {
		return logger;
	}

}
