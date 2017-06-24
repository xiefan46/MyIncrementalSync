package com.alibaba.middleware.race.sync.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.map.ArrayHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.RAFOutputStream;
import com.generallycloud.baseio.common.CloseUtil;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

	private static final Logger	logger				= LoggerFactory
			.getLogger(RecordUtil.class);

	private static final byte	FIELD_SEPERATOR_BYTE	= '\t';

	private static final byte	FIELD_N_BYTE			= '\n';

	private static final int		LONG_LEN				= String.valueOf(Long.MAX_VALUE)
			.length() - 1;

	private static final byte[]	ID_CACHE				= new byte[LONG_LEN + 1];

	private static final byte[]	NUM_MAPPING			= new byte[] { '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9' };

	public static void formatResultString(int cols, long id, byte[] record, ByteBuffer buffer) {
		buffer.clear();
		byte[] idCache = ID_CACHE;
		int off = valueOfLong(id, idCache);
		buffer.put(idCache, off + 1, LONG_LEN - off);
		buffer.put(FIELD_SEPERATOR_BYTE);
		int len = cols - 1;
		for (byte i = 0; i < len; i++) {
			int tOff = i * 8 + 1;
			int tLen = record[1 + tOff++];
			buffer.put(record, 1 + tOff, tLen);
			buffer.put(FIELD_SEPERATOR_BYTE);
		}
		int tOff = len * 8 + 1;
		int tLen = record[1 + tOff++];
		buffer.put(record, 1 + tOff, tLen);
		buffer.put(FIELD_N_BYTE);
	}

	private static int valueOfLong(long v, byte[] array) {
		long v1 = v;
		int off = LONG_LEN;
		byte[] NUM_MAPPING1 = NUM_MAPPING;
		for (;;) {
			if (v1 == 0) {
				return off;
			}
			array[off--] = NUM_MAPPING1[(int) (v1 % 10)];
			v1 /= 10;
		}
	}

	public static void writeResultToLocalFile(Context context, String fileName) throws Exception {

		ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(1024 * 128);

		RecordUtil.writeToByteArrayBuffer(context, byteArrayBuffer);

		writeToFile(byteArrayBuffer, fileName);

	}

	public static void writeToByteArrayBuffer(Context context, OutputStream buffer)
			throws IOException {
		int startId = 0;
		int endId = 0;
		int all = 0;
		int cols = 0;
		ByteBuffer array = ByteBuffer.allocate(1024 * 1024 * 1);
		ArrayHashMap resultMap = null;
		if (endId >= resultMap.MAX_NUMBER) {
			throw new RuntimeException("数组太小");
		}
		byte[][] resultArray = resultMap.getResultsArray();
		for (int i = startId + 1; i < endId; i++) {
			byte[] r = resultArray[i];
			if (r == null || r[0] != (byte) 1) {
				continue;
			}
			//			if (r.getAlterType() != Constants.INSERT) {
			//				throw new RuntimeException(
			//						"Error alter type in result. Type : " + (char) r.getAlterType());
			//			}
			all++;
			RecordUtil.formatResultString(cols, i, r, array);
			buffer.write(array.array(), 0, array.position());
		}
		logger.info("result size:{}", all);
	}

	public static void writeToFile(ByteArrayBuffer buffer, String fileName) throws IOException {
		RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
		RAFOutputStream outputStream = new RAFOutputStream(file);
		outputStream.write(buffer.array(), 0, buffer.size());
		CloseUtil.close(outputStream);
	}

}
