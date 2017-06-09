package com.alibaba.middleware.race.sync.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.RecalculateContext;
import com.alibaba.middleware.race.sync.channel.RAFOutputStream;
import com.alibaba.middleware.race.sync.model.Record;
import com.generallycloud.baseio.common.CloseUtil;
import com.generallycloud.baseio.component.ByteArrayBuffer;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

	private static final byte	FIELD_SEPERATOR_BYTE	= '\t';

	private static final byte	FIELD_N_BYTE			= '\n';

	private static final Logger	logger				= LoggerUtil.SERVER_LOGGER;

	public static void formatResultString(Record record, ByteBuffer buffer) {
		buffer.clear();
		byte[][] array = record.getColumns();
		byte len = (byte) (array.length - 1);
		for (byte i = 0; i < len; i++) {
			buffer.put(array[i]);
			buffer.put(FIELD_SEPERATOR_BYTE);
		}
		buffer.put(array[len]);
		buffer.put(FIELD_N_BYTE);
	}

	public static void writeResultToLocalFile(Context context, String fileName) throws Exception {
		long startTime = System.currentTimeMillis();

		ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(1024 * 128);

		RecordUtil.writeToByteArrayBuffer(context, byteArrayBuffer);

		writeToFile(byteArrayBuffer, fileName);

		logger.info("写结果文件到本地文件系统耗时 : {}", System.currentTimeMillis() - startTime);
	}

	public static void writeToByteArrayBuffer(Context context, ByteArrayBuffer buffer) {
		List<Record> result = getResult(context);
		logger.info("结果文件条目数 : " + result.size());
		ByteBuffer array = ByteBuffer.allocate(1024 * 1024 * 1);
		for (Record r : result) {
			RecordUtil.formatResultString(r, array);
			buffer.write(array.array(), 0, array.position());
		}
		byte[] bytes = buffer.array();
		String str = new String(bytes, 0, buffer.size());
		logger.info("打印结果文件: ");
		logger.info(str);
	}


	private static List<Record> getResult(Context context) {
		long startId = context.getStartId();
		long endId = context.getEndId();
		List<Record> records = new ArrayList<>();
		RecalculateContext rContext = context.getRecalculateContext();
		for (long i = startId + 1; i < endId; i++) {
			Record r = rContext.getRecords().get(i);
			if (r == null) {
				continue;
			}
			records.add(r);
		}
		return records;
	}

	public static void writeToFile(ByteArrayBuffer buffer, String fileName) throws IOException {
		RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
		RAFOutputStream outputStream = new RAFOutputStream(file);
		outputStream.write(buffer.array(), 0, buffer.size());
		CloseUtil.close(outputStream);
	}

}
