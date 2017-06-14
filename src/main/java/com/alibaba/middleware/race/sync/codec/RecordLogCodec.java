package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Constants;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 */
public class RecordLogCodec {

	private static RecordLogCodec	recordLogCodec	= new RecordLogCodec();
	
	private final int			U_D_SKIP		= "1:1|X".length();
	
	private final int			I_SKIP		= "1:1|NULL|X".length();
	
	private final int			HEAD_SKIP		= "|mysql-bin.".length();
	
	private final int			TIME_SKIP		= "1496720884000".length() + 1;
	
	public static RecordLogCodec get() {
		return recordLogCodec;
	}
	
	private RecordLogCodec(){}
	
	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	public RecordLog decode(byte[] data,byte [] tableSchema, int offset, int last,int cols) {
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
		off += TIME_SKIP;
//		if (!compare(data, off + 1, tableSchema)) {
//			return null;
//		}
		RecordLog r = new RecordLog();
		int end;
		off = off + tableSchema.length + 2;
		byte alterType = data[off];
		r.setAlterType(alterType);
		off += 2;
		if (Constants.UPDATE == alterType) {
			r.newColumns(cols);
			for (;;) {
				end = findNextChar(data, off, ':');
				if (data[end + 3] == '1') {
					PrimaryColumnLog c = new PrimaryColumnLog();
					r.setPrimaryColumn(c);
//					c.setName(data, off, end - off);
					off = end + U_D_SKIP;
					end = findNextChar(data, off, '|');
					c.setBeforeValue(parseLong(data, off, end));
					off = end + 1;
					end = findNextChar(data, off, '|');
					c.setLongValue(parseLong(data, off, end));
//					c.setValue(data,off,end-off);
					off = end + 1;
					if (off >= last) {
						return r;
					}
					continue;
				}
				ColumnLog c = new ColumnLog();
				c.setName(data, off, end - off);
				r.addColumn(c);
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				c.setValue(data, off, end - off);
				off = end + 1;
				if (off >= last) {
					return r;
				}
			}
		}

		if (Constants.DELETE == alterType) {
			end = findNextChar(data, off, ':');
			PrimaryColumnLog c = new PrimaryColumnLog();
//			c.setName(data, off, end - off);
			r.setPrimaryColumn(c);
			off = end + U_D_SKIP;
			end = findNextChar(data, off, '|');
			c.setLongValue(parseLong(data, off, end));
//			c.setValue(data,off,end-off);
			return r;
		}

		if (Constants.INSERT == alterType) {
			r.newColumns(cols);
			for (;;) {
				end = findNextChar(data, off, ':');
				if (data[end + 3] == '1') {
					PrimaryColumnLog c = new PrimaryColumnLog();
					r.setPrimaryColumn(c);
//					c.setName(data, off, end - off);
					off = end + I_SKIP;
					end = findNextChar(data, off, '|');
					c.setLongValue(parseLong(data, off, end));
//					c.setValue(data,off,end-off);
					off = end + 1;
					if (off >= last) {
						return r;
					}
					continue;
				}
				ColumnLog c = new ColumnLog();
				c.setName(data, off, end - off);
				r.addColumn(c);
				off = end + I_SKIP;
				end = findNextChar(data, off, '|');
				c.setValue(data, off, end - off);
				off = end + 1;
				if (off >= last) {
					return r;
				}
			}

		}
		throw new RuntimeException(String.valueOf(alterType));
	}

	private int findNextChar(byte[] data, int offset, char c) {
		for (;;) {
			if (data[++offset] == c) {
				return offset;
			}
		}
	}

	private long parseLong(byte[] data, int offset, int end) {
		long all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}

	public void initialize() {

	}

}
