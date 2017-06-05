package com.alibaba.middleware.race.sync;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.io.FixedLengthReadFuture;
import com.alibaba.middleware.race.sync.io.FixedLengthReadFutureImpl;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.util.RecordUtil;
import com.generallycloud.baseio.common.CloseUtil;
import com.generallycloud.baseio.component.ByteArrayBuffer;
import com.generallycloud.baseio.component.SocketChannelContext;
import com.generallycloud.baseio.component.SocketSession;

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
		for (int i = 1; i < channels.length; i++) {
			Context c = contexts[i];
			for (Record r : c.getRecords().values()) {
				receiver.receivedFinal(finalContext, r);
			}
		}

		String fileName = Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME;

		ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(1024 * 128);

		writeToByteArrayBuffer(finalContext, byteArrayBuffer);

		writeToFile(byteArrayBuffer, fileName);
		
		writeToClient(byteArrayBuffer);

		//TODO send to client
		//write to local file system for debug
		//		writeToLocal(finalContext);
	}
	
	private void writeToClient(ByteArrayBuffer buffer){
		
		SocketChannelContext channelContext = Server.get().getSocketChannelContext();
		
		Map<Integer, SocketSession>  sessions = channelContext.getSessionManager().getManagedSessions();
		
		if (sessions.size() == 0) {
			throw new RuntimeException("null client");
		}
		
		SocketSession session = sessions.values().iterator().next();
		
		FixedLengthReadFuture future = new FixedLengthReadFutureImpl(channelContext);
		
		future.write(buffer.array(), 0, buffer.size());
		
		session.flush(future);
	}

	private void writeToByteArrayBuffer(Context context, ByteArrayBuffer buffer) {
		//sort
		TreeMap<Long, Record> finalResult = new TreeMap<>();
		for (Map.Entry<Long, Record> entry : context.getRecords().entrySet()) {
			finalResult.put(entry.getKey(), entry.getValue());
		}
		logger.debug("Final result size : {}", context.getRecords().size());
		ByteBuffer array = ByteBuffer.allocate(1024 * 1024 * 4);
		StringBuilder sb = new StringBuilder(1024 * 1024);
		for (Record r : finalResult.values()) {
			RecordUtil.formatResultString(r,sb, array);
			buffer.write(array.array(), 0, array.position());
		}
	}

	private void writeToFile(ByteArrayBuffer buffer, String fileName) throws IOException {
		RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
		RAFOutputStream outputStream = new RAFOutputStream(file);
		outputStream.write(buffer.array(), 0, buffer.size());
		CloseUtil.close(outputStream);
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
			logger.debug("Final result size : {}", context.getRecords().size());
			for (Map.Entry<Long, Record> entry : finalResult.entrySet()) {
				String formatRecord = RecordUtil.formatResultString(entry.getValue());
				logger.debug("Write result : {}", formatRecord);
				formatRecord += "\n";
				bos.write(formatRecord.getBytes());
			}
			bos.flush();
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
