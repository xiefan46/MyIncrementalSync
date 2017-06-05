package com.alibaba.middleware.race.sync.util;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Record;

import static com.google.common.base.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordUtil {

	private static final String FIELD_SEPERATOR = "\t";
	
	private static CharsetEncoder encoder = Charset.defaultCharset().newEncoder();

	public static String formatResultString(Record record) {
		checkState(record.getAlterType() == Record.INSERT,
				"Fail to format result because of wrong alter type");
		StringBuilder sb = new StringBuilder();
		sb.append(record.getPrimaryColumn().getValue());
		for (Column c : record.getColumns().values()) {
			sb.append(FIELD_SEPERATOR);
			sb.append(c.getValue());
		}
		return sb.toString();
	}
	
	
	/**
	 * 
	 * @param record
	 * @param array
	 * @param offset
	 * @return new offset
	 */
	public static void formatResultString(Record record,StringBuilder sb ,ByteBuffer buffer) {
		checkState(record.getAlterType() == Record.INSERT,
				"Fail to format result because of wrong alter type");
		sb.setLength(0);
		sb.append(record.getPrimaryColumn().getValue());
		for (Column c : record.getColumns().values()) {
			sb.append(FIELD_SEPERATOR);
			sb.append(c.getValue());
		}
		buffer.clear();
		CoderResult cr = encoder.encode(CharBuffer.wrap(sb), buffer,true);
		if (cr.isError()) {
			try {
				cr.throwException();
			} catch (CharacterCodingException ex) {
				throw new RuntimeException(ex);
			}
		}
		encoder.reset();
	}
	
}
