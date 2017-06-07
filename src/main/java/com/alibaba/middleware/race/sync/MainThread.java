package com.alibaba.middleware.race.sync;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import com.alibaba.middleware.race.sync.util.LoggerUtil;
import org.slf4j.Logger;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.Statistics;
import com.alibaba.middleware.race.sync.stream.RAFInputStream;

/**
 * @author wangkai
 */
public class MainThread implements Runnable {

	private static final Logger	logger				= LoggerUtil.getServerLogger();

	private RecordLogReceiver	receiver;
	private String				schema;
	private String				table;
	private long				startId;
	private long				endId;
	private boolean			executeByCoreProcesses	= false;
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
		logger.info("----------MainThread start-------------");
		logger.info("----------MainThread start-------------");
		logger.info("----------MainThread start-------------");
		long startTime = System.currentTimeMillis();
		String tableSchema = (schema + "|" + table);
		File root = new File(Constants.DATA_HOME);
		File[] files = getAllDataFiles(root);
		logger.info("files num : " + files.length);
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
			channels = initChannelsByCoreProcesses(root, coreProcesses);
		} else {
			contexts = new Context[files.length];
			ts = new Thread[files.length];
			channels = initChannelsByFiles(files, files.length);
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

		//print stat
		if (Constants.COLLECT_STAT) {
			Statistics finalStat = new Statistics(contexts[0].getStartId(),
					contexts[0].getEndId());
			for (int i = 0; i < channels.length; i++) {
				logger.info("print stat : {}", i);
				contexts[i].getStat().printStat();
				logger.info("-----------print stat end----------");
				finalStat = Statistics.combine(finalStat, contexts[i].getStat());
			}

			logger.info("------------print final stat ------------");
			finalStat.printStat();
			logger.info("-------------print final stat end---------");
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
		startTime = System.currentTimeMillis();
		this.finalContext = contexts[0];
		Context _finalContext = finalContext;
		for (int i = 1; i < channels.length; i++) {
			Context c = contexts[i];
			for (Record r : c.getRecords().values()) {
				receiver.receivedFinal(_finalContext, r, startId, endId);
			}
		}

		logger.info("合并各个线程结果耗时 : {}. 记录总数 : {}", System.currentTimeMillis() - startTime,
				finalContext.getRecords().size());
		logger.info("----------MainThread end-------------");
		logger.info("----------MainThread end-------------");
		logger.info("----------MainThread end-------------");
	}

	public Context getFinalContext() {
		checkNotNull(finalContext, "最终结果未计算完成");
		return finalContext;
	}

	private ReadChannel[] initChannelsByFiles(File[] files, int size) throws IOException {
		ReadChannel[] rcs = new ReadChannel[files.length];
		Arrays.sort(files);
		for (int i = 0; i < files.length; i++) {
			File f = files[i];
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			rcs[i] = new SimpleReadChannel(new RAFInputStream(raf), 256 * 1024);
			logger.info("File channel ok. File name : {}. File size : {} B", f.getName(),
					f.length());
		}
		return rcs;
	}

	private ReadChannel[] initChannelsByCoreProcesses(File root, int size) throws IOException {
		return CompoundReadChannelSplitor.split(root, size);
	}

	private File[] getAllDataFiles(File root) {
		File[] files = root.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(".txt"))
					return true;
				return false;
			}
		});
		return files;
	}

}
