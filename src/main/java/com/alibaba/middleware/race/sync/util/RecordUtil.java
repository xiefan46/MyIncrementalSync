package com.alibaba.middleware.race.sync.util;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.ReadRecordLogContext;
import com.alibaba.middleware.race.sync.channel.RAFOutputStream;
import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Record;
import com.generallycloud.baseio.common.CloseUtil;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;
import com.generallycloud.baseio.component.ByteArrayBuffer;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

	private static final char	FIELD_SEPERATOR	= '\t';
	
	private static final byte FIELD_SEPERATOR_BYTE = '\t';
	
	private static CharsetEncoder	encoder			= Charset.defaultCharset().newEncoder();

	private static final Logger	logger			= LoggerFactory.getLogger(RecordUtil.class);

	public static void formatResultString(Record record, ByteBuffer buffer) {
		checkState(record.getAlterType() == Record.INSERT,
				"Fail to format result because of wrong alter type");
		buffer.clear();
		buffer.put(record.getPrimaryColumn().getValueBytes());
		for (Column c : record.getColumns().values()) {
			buffer.put(FIELD_SEPERATOR_BYTE);
			buffer.put(c.getValueBytes());
		}
	}

	public static void writeResultToLocalFile(Context context, String fileName)
			throws Exception {
		long startTime = System.currentTimeMillis();

		ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(1024 * 128);

		RecordUtil.writeToByteArrayBuffer(context, byteArrayBuffer);

		writeToFile(byteArrayBuffer, fileName);

		logger.info("写结果文件到本地文件系统耗时 : {}", System.currentTimeMillis() - startTime);
	}

	public static void writeToByteArrayBuffer(Context context, ByteArrayBuffer buffer) {
		//sort
		TreeMap<Long, Record> finalResult = new TreeMap<>();
		for (Map.Entry<Long, Record> entry : context.getRecords().entrySet()) {
			finalResult.put(entry.getKey(), entry.getValue());
		}
		ByteBuffer array = ByteBuffer.allocate(1024 * 1024 * 1);
		for (Record r : finalResult.values()) {
			RecordUtil.formatResultString(r, array);
			buffer.write(array.array(), 0, array.position());
		}
	}

	public static void writeToFile(ByteArrayBuffer buffer, String fileName) throws IOException {
		RandomAccessFile file = new RandomAccessFile(new File(fileName), "rw");
		RAFOutputStream outputStream = new RAFOutputStream(file);
		outputStream.write(buffer.array(), 0, buffer.size());
		CloseUtil.close(outputStream);
	}

}
