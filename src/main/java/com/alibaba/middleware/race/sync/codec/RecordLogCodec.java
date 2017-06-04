package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 *
 */
public class RecordLogCodec {
	
	private static RecordLogCodec recordLogCodec = new RecordLogCodec();
	
	public static RecordLogCodec get(){
		return recordLogCodec;
	}
	
	public Record decode(byte[] data, int offset, int last) {
		Record r = new Record();
//		r.setTimestamp(parseLong(data, offset + 11, offset + 24));
//		int off = offset + 25;
//		int end = findNextChar(data, off, '|');
//		end = findNextChar(data, end + 1, '|');
//		r.setTableSchema(new String(data, off, end - off));
//		off = end + 1;
		int off = offset;
		int end;
		r.setAlterType(data[off]);
		off += 2;
		if ('U' == r.getAlterType()) {
			r.newColumns();
			for (;;) {
				end = findNextChar(data, off, ':');
				Column c = new Column();
				c.setName(new String(data, off, end - off));
				if (data[end + 3] == '1') {
					r.setPrimaryColumn(c);
					c.setPrimary(true);
				}else{
					r.addColumn(c);
				}
				boolean isNumber = data[end + 1] == '1';
				off = end + 5;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				if (isNumber) {
					c.setValue(parseLong(data, off, end));
				} else {
					c.setValue(new String(data, off, end - off));
				}
				off = end + 1;
				if (off == last) {
					return r;
				}
			}
		}

		if ('D' == r.getAlterType()) {
			end = findNextChar(data, off, ':');
			Column c = new Column();
			c.setName(new String(data, off, end - off));
			r.setPrimaryColumn(c);
			c.setPrimary(true);
//			if (data[end + 1] == '1') {
//				r.setPrimaryColumn(c);
//				c.setPrimary(true);
//			}else{
//				r.addColumn(c);
//			}
			boolean isNumber = data[end + 1] == '1';
			off = end + 5;
			end = findNextChar(data, off, '|');
			if (isNumber) {
				c.setValue(parseLong(data, off, end));
			} else {
				c.setValue(new String(data, off, end - off));
			}
			return r;
		}

		if ('I' == r.getAlterType()) {
			r.newColumns();
			for (;;) {
				end = findNextChar(data, off, ':');
				Column c = new Column();
				c.setName(new String(data, off, end - off));
				if (data[end + 3] == '1') {
					r.setPrimaryColumn(c);
					c.setPrimary(true);
				}else{
					r.addColumn(c);
				}
				boolean isNumber = data[end + 1] == '1';
				off = end + 6;
				end = findNextChar(data, off, '|');
				if (isNumber) {
					c.setValue(parseLong(data, off, end));
				} else {
					c.setValue(new String(data, off, end - off));
				}
				off = end + 1;
				if (off == last) {
					return r;
				}
			}
		}

		return r;
	}

	private long parseLong(byte[] data, int offset, int end) {
		long all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}
	
	private int findNextChar(byte[] data, int offset, char c) {
		for (;;) {
			if (data[++offset] == c) {
				return offset;
			}
		}
	}

	private int parseInt(byte[] data, int offset, int end) {
		int all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}

	public int encode(Record record) {
		throw new UnsupportedOperationException();
	}

	public void initialize() {
		
	}

}
