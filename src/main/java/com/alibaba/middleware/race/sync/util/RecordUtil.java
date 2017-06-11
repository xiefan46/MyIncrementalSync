package com.alibaba.middleware.race.sync.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
	
	private static final int	LONG_LEN 			= String.valueOf(Long.MAX_VALUE).length() -1; 

	private static final byte[]	ID_CACHE				= new byte[LONG_LEN + 1];
	
	private static final byte[] NUM_MAPPING			= new byte[]{'0','1','2','3','4','5','6','7','8','9'};  

	public static void formatResultString(long id,Record record, ByteBuffer buffer) {
		buffer.clear();
		byte [] idCache = ID_CACHE;
		int off = valueOfLong(id, idCache);
		buffer.put(idCache, off+1, LONG_LEN - off);
		buffer.put(FIELD_SEPERATOR_BYTE);
		byte[][] array = record.getColumns();
		byte len = (byte) (array.length - 1);
		for (byte i = 0; i < len; i++) {
			buffer.put(array[i]);
			buffer.put(FIELD_SEPERATOR_BYTE);
		}
		buffer.put(array[len]);
		buffer.put(FIELD_N_BYTE);
	}
	
	private static int valueOfLong(long v,byte [] array){
		long v1 = v;
		int off = LONG_LEN;
		byte[] NUM_MAPPING1 = NUM_MAPPING;
		for(;;){
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
		long startId = context.getStartId();
		long endId = context.getEndId();
		RecalculateContext rContext = context.getRecalculateContext();
		ByteBuffer array = ByteBuffer.allocate(1024 * 1024 * 1);
		for (long i = startId + 1; i < endId; i++) {
			Record r = rContext.getRecords().get(i);
			if (r == null) {
				continue;
			}
			RecordUtil.formatResultString(i, r, array);
			buffer.write(array.array(), 0, array.position());
		}
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
