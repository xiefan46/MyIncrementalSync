package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.RAFInputStream;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.channel.SimpleReadChannel;

/**
 * @author wangkai
 */
public class MainThread implements Runnable {

	private Logger				logger	= LoggerFactory.getLogger(getClass());

	private ReadChannel[]		channels;

	private int				threadNum;

	private Context[]			allContext;

	private ReadRecordLogThread[]	threads;

	private Context			finalContext;

	private Table				table;

	public MainThread(long startId, long endId, RecordLogReceiver recordLogReceiver,
			String tableSchema, int threadNum) throws IOException {
		this.threadNum = threadNum;
		init(startId, endId, recordLogReceiver, tableSchema);
	}

	@Override
	public void run() {
		execute();
	}

	private void init(long startId, long endId, RecordLogReceiver recordLogReceiver,
			String tableSchema) throws IOException {
		long startTime = System.currentTimeMillis();
		File root = new File(Constants.DATA_HOME);
		if (!root.exists()) {
			throw new RuntimeException("数据文件目录不存在");
		}
		table = getTableStructure(root, tableSchema);
		channels = initMultiChannels(root, threadNum);
		threads = new ReadRecordLogThread[threadNum];
		allContext = new Context[threadNum];
		for (int i = 0; i < threadNum; i++) {
			allContext[i] = new Context(endId, recordLogReceiver, startId, tableSchema,
					channels[i], table);
			threads[i] = new ReadRecordLogThread(allContext[i]);
		}
		logger.info("MainThread初始化耗时 : {}", System.currentTimeMillis() - startTime);
	}

	public void execute() {
		try {
			logger.info("--------------Main thread start-----------");
			long startTime = System.currentTimeMillis();
			multiThreadScan();
			logger.info("等待所有线程完成耗时 : {}", System.currentTimeMillis() - startTime);
			startTime = System.currentTimeMillis();
			combineResult();
			logger.info("合并结果耗时 : {}", System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void multiThreadScan() throws Exception {
		Thread[] allThread = new Thread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			allThread[i] = new Thread(threads[i]);
			allThread[i].start();
		}
		for (int i = 0; i < threadNum; i++) {
			allThread[i].join();
		}
	}

	private Table getTableStructure(File root, String tableSchema) throws IOException {
		ReadChannel channel = MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/",
				1, 1, 1024 * 128);
		ChannelReader channelReader = ChannelReader.get();
		byte[] tableSchemaBytes = tableSchema.getBytes();
		for (; channel.hasRemaining();) {

			RecordLog r = channelReader.read(channel, tableSchemaBytes, 8);

			if (r == null) {
				continue;
			}

			Table t = Table.newTable(r);

			channel.close();

			return t;
		}
		throw new RuntimeException("无法确定表结构");
	}

	private void combineResult() throws Exception {
		long startTime = System.currentTimeMillis();
		finalContext = allContext[0];
		RecordLogReceiver receiver = new RecordLogReceiverImpl();
		for (int i = 1; i < threadNum; i++) {
			System.out.println("receive final");
			for (Map.Entry<Long, Record> entry : allContext[i].getRecords().entrySet()) {
				receiver.receivedFinal(finalContext, entry.getKey(), entry.getValue());
			}
		}
		logger.info("Combine final result cost time : {}",
				System.currentTimeMillis() - startTime);
	}

	private ReadChannel initChannels2() throws IOException {
		File root = new File(Constants.DATA_HOME);
		return MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/", 1, 10,
				1024 * 128);
	}

	private ReadChannel initChannels3() throws IOException {
		File root = new File(Constants.TESTER_HOME + "/canal.txt");
		RandomAccessFile raf = new RandomAccessFile(root, "r");
		RAFInputStream inputStream = new RAFInputStream(raf);
		return new SimpleReadChannel(inputStream, 1024 * 128);
	}

	private ReadChannel[] initMultiChannels(File root, int threadNum) throws IOException {
		ReadChannel[] channels = new ReadChannel[threadNum];
		if (10 % threadNum != 0) {
			throw new RuntimeException("请选择能被10整除的线程数");
		}
		int fileNum = 10 / threadNum;
		for (int i = 0; i < threadNum; i++) {
			channels[i] = MuiltFileReadChannelSplitor.newChannel(root.getAbsolutePath() + "/",
					i * fileNum + 1, fileNum, 1024 * 128);
		}
		return channels;
	}

	public Context getFinalContext() {
		return finalContext;
	}

	public void setFinalContext(Context finalContext) {
		this.finalContext = finalContext;
	}
}
