package com.alibaba.middleware.race.sync.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.channel.ByteArrayBuffer;
import com.alibaba.middleware.race.sync.channel.RAFOutputStream;
import com.generallycloud.baseio.common.CloseUtil;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

//	private static final Logger	logger				= LoggerUtil.get();

	private static final byte	FIELD_SEPERATOR_BYTE	= '\t';

	private static final byte	FIELD_N_BYTE			= '\n';

	private static final int		LONG_LEN				= String.valueOf(Long.MAX_VALUE)
			.length() - 1;

	private static final byte[]	ID_CACHE				= new byte[LONG_LEN + 1];

	private static final byte[]	NUM_MAPPING			= new byte[] { '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9' };

	public static int formatResultString(byte[] src,int pk,int srcOff,int cols,byte[] target) {
		byte[] idCache = ID_CACHE;
		int pkOff = valueOfLong(pk, idCache);
		int off = LONG_LEN - pkOff;
		arrayCopy(idCache, pkOff + 1, target, 0, off);
		target[off++] = FIELD_SEPERATOR_BYTE;
		int len = cols - 1;
		for (byte i = 0; i < len; i++) {
			int tOff = i*8 + srcOff;
			int tLen = src[tOff++];
			arrayCopy(src, tOff, target, off, tLen);
			off += tLen;
			target[off++] = FIELD_SEPERATOR_BYTE;
		}
		int tOff = len*8 + srcOff;
		int tLen = src[tOff++];
		arrayCopy(src, tOff, target, off, tLen);
		off += tLen;
		target[off++] = FIELD_N_BYTE;
		return off;
	}
	
	private static void arrayCopy(byte [] src,int srcOff,byte [] target,int targetOff,int len){
		int targetLen = targetOff + len;
		for (int i = targetOff; i < targetLen; i++) {
			target[targetOff] = src[srcOff++];
		}
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

	public static void writeToByteArrayBuffer(Context context, OutputStream buffer) throws IOException {
//		long start = System.currentTimeMillis();
		int startId = (int) context.getStartId();
		int endId = (int) context.getEndId();
//		int all = 0;
		int cols = context.getTable().getColumnSize();
		byte [] array = new byte [1024];
		RecordMap recordMap = context.getRecordMap();
		byte [] data = recordMap.getData();
		for (int i = startId + 1; i < endId; i++) {
			int off = recordMap.getResult(i);
			if (off == -1) {
				continue;
			}
//			all++;
			int len = RecordUtil.formatResultString(data, i,off, cols, array);
			buffer.write(array, 0, len);
		}
//		logger.info("result size:{}. 写结果Buffer耗时 : {}", all,System.currentTimeMillis() - start);
	}

	public static void writeToFile(ByteArrayBuffer buffer, String fileName) throws IOException {
		RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
		RAFOutputStream outputStream = new RAFOutputStream(file);
		outputStream.write(buffer.array(), 0, buffer.size());
		CloseUtil.close(outputStream);
	}

}
