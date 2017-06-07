package com.alibaba.middleware.race.sync;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

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
	private boolean			executeByCoreProcesses = false;
	private Context			finalContext;

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
		long startTime = System.currentTimeMillis();
		String tableSchema = (schema + "|" + table);
		File root = new File(Constants.DATA_HOME);
		File[] files = root.listFiles();
		logger.debug("files num : " + files.length);
		if (!root.exists()) {
			throw new FileNotFoundException(root.getAbsolutePath());
		}
		ReadChannel[] channels;
		Thread[] ts;
		Context[] contexts;
		if (executeByCoreProcesses) {
			int coreProcesses = Runtime.getRuntime().availableProcessors();
			contexts = new Context[coreProcesses];
			ts = new Thread[coreProcesses];
			channels = initChannelsByCoreProcesses(root,coreProcesses);
		}else{
			contexts = new Context[files.length];
			ts = new Thread[files.length];
			channels = initChannelsByFiles(root,files.length);
		}
		logger.info("Thread num : {}", ts.length);
		for (int i = 0; i < ts.length; i++) {
			contexts[i] = new Context(channels[i], endId, receiver, startId, tableSchema);
			contexts[i].initialize();
			ts[i] = new Thread(new ReadRecordLogThread(contexts[i]));
		}
		logger.info("MainThread 初始化耗时 : {}", System.currentTimeMillis() - startTime);
		startTime = System.currentTimeMillis();
		for (int i = 0; i < ts.length; i++) {
			ts[i].start();
		}
		for (int i = 0; i < ts.length; i++) {
			ts[i].join();
		}
		logger.info("解析记录耗时 : {}", System.currentTimeMillis() - startTime);

		// 倒序
		//		Context finalContext = contexts[contexts.length - 1];
		//		for (int i = contexts.length - 2; i > 0; i--) {
		//			Context c = contexts[i];
		//			for (Record r : c.getRecords().values()) {
		//				receiver.received(finalContext, r);
		//			}
		//		}

		// 正序
		startTime = System.currentTimeMillis();
		this.finalContext = contexts[0];
		Context _finalContext = finalContext;
		for (int i = 1; i < channels.length; i++) {
			Context c = contexts[i];
			for (Record r : c.getRecords().values()) {
				receiver.receivedFinal(_finalContext, r,startId,endId);
			}
		}
		
		logger.info("合并各个线程结果耗时 : {}. 记录总数 : {}", System.currentTimeMillis() - startTime,
				finalContext.getRecords().size());

	}
	
	public Context getFinalContext() {
		checkNotNull(finalContext, "最终结果未计算完成");
		return finalContext;
	}

	private ReadChannel[] initChannelsByFiles(File root,int size) throws IOException {
		File[] files = root.listFiles();
		ReadChannel[] rcs = new ReadChannel[files.length];
		Arrays.sort(files);
		for (int i = 0; i < files.length; i++) {
			File f = files[i];
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			rcs[i] = new SimpleReadChannel(new RAFInputStream(raf), 256 * 1024);
			logger.info("File channel ok. File name : {}. File size : {} B", f.getName(),f.length());
		}
		return rcs;
	}
	
	private ReadChannel[] initChannelsByCoreProcesses(File root,int size) throws IOException {
		return CompoundReadChannelSplitor.split(root, size);
	}

}
