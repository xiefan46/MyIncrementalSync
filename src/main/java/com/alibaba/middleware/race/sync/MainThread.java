package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 *
 */
public class MainThread implements Runnable {

	private Logger				logger	= LoggerFactory.getLogger(getClass());

	private RecordLogReceiver	receiver;
	private String				schema;
	private String				table;
	private int				startId;
	private int				endId;

	public MainThread(RecordLogReceiver receiver, String schema, String table, int startId,
			int endId) {
		this.receiver = receiver;
		this.schema = schema;
		this.table = table;
		this.startId = startId;
		this.endId = endId;
	}

	@Override
	public void run() {
		try {
			startup(receiver, schema, table, startId, endId);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void startup(RecordLogReceiver receiver, String schema, String table, int startId,
			int endId) throws Exception {

		String tableSchema = (schema + "|" + table);
		int coreProcesses = Runtime.getRuntime().availableProcessors();
		File root = new File(Constants.DATA_HOME);
		if (!root.exists()) {
			throw new FileNotFoundException(root.getAbsolutePath());
		}
		Context[] contexts = new Context[coreProcesses];
		Thread[] ts = new Thread[coreProcesses];
		ReadChannel[] channels = initChannels(root);
		for (int i = 0; i < ts.length; i++) {
			contexts[i] = new Context(channels[i], endId, receiver, startId, tableSchema);
			contexts[i].initialize();
			ts[i] = new Thread(new ReadRecordLogThread(contexts[i]));
		}
		for (int i = 0; i < ts.length; i++) {
			ts[i].start();
		}
		for (int i = 0; i < ts.length; i++) {
			ts[i].join();
		}

		// 倒序
//		Context finalContext = contexts[contexts.length - 1];
//		for (int i = contexts.length - 2; i > 0; i--) {
//			Context c = contexts[i];
//			for (Record r : c.getRecords().values()) {
//				receiver.received(finalContext, r);
//			}
//		}
		
		// 正序
		Context finalContext = contexts[0];
		for (int i = 0; i < channels.length; i++) {
			Context c = contexts[i];
			for (Record r : c.getRecords().values()) {
				receiver.received(finalContext, r);
			}
		}
		
		//TODO send to client
	}

	private ReadChannel[] initChannels(File root) {
		throw new UnsupportedOperationException();
	}

}
