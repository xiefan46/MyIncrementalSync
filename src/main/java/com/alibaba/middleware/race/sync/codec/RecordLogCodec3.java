package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 */
public class RecordLogCodec3 implements RecordLogCodec {

	private static RecordLogCodec	recordLogCodec	= new RecordLogCodec3();
	
	private final int			U_D_SKIP		= "1:1|X".length();
	
	private final int			I_SKIP		= "1:1|NULL|X".length();
	
	private final int			HEAD_SKIP		= "|mysql-bin.".length() + 0;
	
	private final int			TIME_SKIP		= "1496720884000".length() + 1;
	
	public static RecordLogCodec get() {
		return recordLogCodec;
	}
	
	private RecordLogCodec3(){}
	
	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int decode(Table table,byte[] data,byte [] tableSchema, int offset,RecordLog r) {
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
		off += TIME_SKIP;
//		if (!compare(data, off + 1, tableSchema)) {
//			return findNextChar(data, offset + tableSchema.length, '\n');
//		}
		int end;
		off = off + tableSchema.length + 2;
		byte alterType = data[off];
		r.setAlterType(alterType);
		off += 2;
		if (Constants.UPDATE == alterType) {
			end = findNextChar(data, off, ':');
			if (data[end + 3] == '1') {
				PrimaryColumnLog c = r.getPrimaryColumn();
//				c.setName(data, off, end - off);
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				c.setBeforeValue(parseLong(data, off, end));
				off = end + 1;
				end = findNextChar(data, off, '|');
				c.setLongValue(parseLong(data, off, end));
//				c.setValue(data,off,end-off);
				off = end + 1;
				if (c.isPkChange()) {
					r.setAlterType(Constants.PK_UPDATE);
				}
				if (data[off] == '\n') {
					return off;
				}
			}
			for (;;) {
				end = findNextChar(data, off, ':');
				ColumnLog c = r.getColumn();
//				r.increamentEdit();
				c.setName(table,data, off, end - off);
//				System.out.println(new String(c.getNameByte()));
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				c.setValue(data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					return off;
				}
			}
		}

		if (Constants.DELETE == alterType) {
			end = findNextChar(data, off, ':');
			PrimaryColumnLog c = r.getPrimaryColumn();
//			c.setName(data, off, end - off);
			off = end + U_D_SKIP;
			end = findNextChar(data, off, '|');
			c.setLongValue(parseLong(data, off, end));
//			c.setValue(data,off,end-off);
			return findNextChar(data, end, '\n');
		}

		if (Constants.INSERT == alterType) {
			int [] skips = table.getColumnSkip();
			byte skipIndex = 0;
			end = findNextChar(data, off, ':');
			if (data[end + 3] == '1') {
				PrimaryColumnLog c = r.getPrimaryColumn();
//				c.setName(data, off, end - off);
				off = end + I_SKIP;
				end = findNextChar(data, off, '|');
				c.setLongValue(parseLong(data, off, end));
//				c.setValue(data,off,end-off);
				off = end + 1;
			}
			for (;;) {
				ColumnLog c = r.getColumn();
				c.setName(skipIndex);
				off = end + skips[skipIndex++];
				end = findNextChar(data, off, '|');
				c.setValue(data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					return off;
				}
			}

		}
		throw new RuntimeException(String.valueOf(alterType));
	}

	private int findNextChar(byte[] data, int offset, char c) {
		for (;data[++offset] != c;) {
		}
		return offset;
	}
	
	private int parseLong(byte[] data, int offset, int end) {
		int all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}

	public void initialize() {

	}

}
