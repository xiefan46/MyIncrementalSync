package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Block;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.RecordLogs;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by xiefan on 6/23/17.
 */
public class ParserPool {

	private static final int	THREAD_NUM		= 4;

	private ExecutorService	executorService	= Executors.newFixedThreadPool(THREAD_NUM);

	private final Context	context;

	private final MergeThread mergeThread;

	public ParserPool(Context context,MergeThread mergeThread) {
		this.context = context;
		this.mergeThread =  mergeThread;
	}

	public void submit(final Block block, final AtomicInteger blockCount) {
		this.executorService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					RecordLogs logs = new RecordLogs();
					logs.setId(block.getId());
					ByteBuf buf = block.getData();
					Table table = context.getTable();
					while (buf.hasRemaining()) {
						RecordLog r = RecordLog.newRecordLog(table.getColumnSize());
						byte[] tableSchema = table.getTableSchemaBytes();
						ByteBufReader reader = ByteBufReader.get();
						for (; buf.hasRemaining();) {
							if (!reader.read(table, buf, tableSchema, r)) {
								continue;
							}
							logs.getLogs().add(r);
							r = RecordLog.newRecordLog(table.getColumnSize());
						}
					}
					mergeThread.submit(logs);
					blockCount.incrementAndGet();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

}
