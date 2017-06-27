package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.Table;

public class ParseThread extends WorkThread {

	private Logger					logger		= LoggerFactory.getLogger(getClass());

	private boolean				workDone;

	private BlockingQueue<ReadTask>	tasks			= new ArrayBlockingQueue<>(2);

	private Context				context;

	private ByteBufReader			byteBufReader	= new ByteBufReader();

	public ParseThread(Context context, int index) {
		super("parse-", index);
		this.context = context;
		this.setWork(true);
	}

	protected void work() throws Exception {
		BlockingQueue<ReadTask>	tasks = this.tasks;
		ReadTask task = tasks.poll(16, TimeUnit.MICROSECONDS);
		if (task == null) {
			return;
		}
		if (task == ReadTask.END_TASK) {
			this.workDone = true;
			this.context.getMainThread().setWorkDone();
			return;
		}
		Table table = context.getTable();
		byte[] tableSchema = table.getTableSchemaBytes();
		ByteBufReader reader = this.byteBufReader;
		reader.read(context, task, tableSchema);
		context.getByteBufPool().free(task);
	}

	public void offerTask(ReadTask task) {
		tasks.offer(task);
	}

	public boolean isDone() {
		return workDone;
	}

	@Override
	Logger getLogger() {
		return logger;
	}

}
