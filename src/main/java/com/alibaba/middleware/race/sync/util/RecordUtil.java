package com.alibaba.middleware.race.sync.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray2;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.channel.RAFOutputStream;
import com.alibaba.middleware.race.sync.other.bytes.ByteArrayBuffer;
import com.generallycloud.baseio.common.CloseUtil;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

	private static final Logger	logger				= LoggerFactory
			.getLogger(RecordUtil.class);

	private static final byte	FIELD_SEPERATOR_BYTE	= '\t';

	private static final byte	FIELD_N_BYTE			= '\n';

	private static final int		LONG_LEN				= String.valueOf(Integer.MAX_VALUE)
			.length() - 1;

	private static final byte[]	ID_CACHE				= new byte[LONG_LEN + 1];

	private static final byte[]	NUM_MAPPING			= new byte[] { '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9' };

	private static boolean		p					= false;

	public static void formatResultString(Table table, int id, Record record, ByteBuffer buffer) {
		buffer.clear();
		byte[] idCache = ID_CACHE;
		int off = valueOfLong(id, idCache);
		buffer.put(idCache, off + 1, LONG_LEN - off);
		buffer.put(FIELD_SEPERATOR_BYTE);

		short[] strCols = record.getStrCols();
		/*if (!p) {
			p = true;
			for (Map.Entry<ByteArray2, Short> entry : table.getColValueToId().entrySet()) {
				byte[] value = entry.getKey().copy();
				System.out.println(
						"id : " + entry.getValue() + " value : " + new String(value));
			}
		}*/
		for (int i = 0; i < strCols.length; i++) {
			byte[] value = table.getColArrayById(strCols[i]).copy();
			/*
			 * if (value == null) { System.out.println("null value"); } else
			 * { System.out.println("id : " + strCols[i] + " value : " + new
			 * String(value)); }
			 */
			buffer.put(value);
			buffer.put(FIELD_SEPERATOR_BYTE);
		}

		int[] numCols = record.getNumberCols();
		byte len = (byte) (numCols.length - 1);
		for (byte i = 0; i < len; i++) {
			byte[] bytes = (numCols[i] + "").getBytes();
			buffer.put(bytes);
			buffer.put(FIELD_SEPERATOR_BYTE);
		}
		buffer.put((numCols[len] + "").getBytes());
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

	public static void writeToByteArrayBuffer(Context context, ByteArrayBuffer buffer) {
		int startId = context.getStartId();
		int endId = context.getEndId();
		int all = 0;
		ByteBuffer array = ByteBuffer.allocate(1024 * 1024 * 1);
		for (int i = startId + 1; i < endId; i++) {
			Record r = context.getRecords().get((int) i);
			if (r == null) {
				continue;
			}
			all++;
			RecordUtil.formatResultString(context.getTable(), i, r, array);
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
