package com.alibaba.middleware.race.sync;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 */
public class MainThread implements Runnable {

	private Logger				logger	= LoggerFactory.getLogger(getClass());

	private RecordLogReceiver	receiver;
	private String				schema;
	private String				table;
	private long				startId;
	private long				endId;

	public MainThread(RecordLogReceiver receiver, String schema, String table, long startId,
			long endId) {
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

	public void startup(RecordLogReceiver receiver, String schema, String table, long startId,
			long endId) throws Exception {

		String tableSchema = (schema + "|" + table);
		File root = new File(Constants.DATA_HOME);
		File[] files = root.listFiles();
		logger.debug("files num : " + files.length);
		if (!root.exists()) {
			throw new FileNotFoundException(root.getAbsolutePath());
		}
		Context[] contexts = new Context[files.length];
		Thread[] ts = new Thread[files.length];
		ReadChannel[] channels = initChannels(root);
		logger.info("Thread num : {}", ts.length);
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
		//write to local file system for debug
		writeToLocal(finalContext);
	}

	public void writeToLocal(Context context) throws Exception {
		BufferedOutputStream bos = null;
		try {
			//sort
			TreeMap<Long, Record> finalResult = new TreeMap<>();
			bos = new BufferedOutputStream(new FileOutputStream(
					Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME));
			for (Map.Entry<Long, Record> entry : context.getRecords().entrySet()) {
				finalResult.put(entry.getKey(), entry.getValue());
			}
			for (Map.Entry<Long, Record> entry : finalResult.entrySet()) {

			}
		} finally {
			if (bos != null)
				bos.close();
		}
	}

	private ReadChannel[] initChannels(File root) throws IOException {
		File[] files = root.listFiles();
		ReadChannel[] rcs = new ReadChannel[files.length];
		Arrays.sort(files);
		for (int i = 0; i < files.length; i++) {
			File f = files[i];
			rcs[i] = new ReadChannel(f.getName(),
					new BufferedInputStream(new FileInputStream(f)), 128 * 1024);
			logger.info("File channel ok. File name : " + f.getName());
		}
		return rcs;
	}

}
