package com.alibaba.middleware.race.sync.codec;

import java.util.List;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Constants;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * @author wangkai
 */
public class RecordLogCodec2 {

	private static RecordLogCodec2	recordLogCodec	= new RecordLogCodec2();
	
	private final int			U_D_SKIP		= "1:1|X".length();
	
	private final int			I_SKIP		= "1:1|NULL|X".length();
	
	private final int			HEAD_SKIP		= "|mysql-bin.".length();
	
	private final int			TIME_SKIP		= "1496720884000".length() + 1;
	
	public static RecordLogCodec2 get() {
		return recordLogCodec;
	}
	
	private RecordLogCodec2(){}
	
	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	private ColumnLog getColumnLog(List<ColumnLog> cols,int index){
		if (index >= cols.size()) {
			cols.add(new ColumnLog());
		}
		return cols.get(index);
	}
	
	public int decode(byte[] data,byte [] tableSchema, int offset,RecordLog r) {
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
		off += TIME_SKIP;
//		if (!compare(data, off + 1, tableSchema)) {
//			return null;
//		}
		int end;
		off = off + tableSchema.length + 2;
		byte alterType = data[off];
		r.setAlterType(alterType);
		off += 2;
		int cIndex = 0;
		List<ColumnLog> columns = r.getColumns();
		if (Constants.UPDATE == alterType) {
			for (;;) {
				end = findNextChar(data, off, ':');
				if (data[end + 3] == '1') {
					PrimaryColumnLog c = r.getPrimaryColumn();
//					c.setName(data, off, end - off);
					off = end + U_D_SKIP;
					end = findNextChar(data, off, '|');
					c.setBeforeValue(parseLong(data, off, end));
					off = end + 1;
					end = findNextChar(data, off, '|');
					c.setLongValue(parseLong(data, off, end));
//					c.setValue(data,off,end-off);
					off = end + 1;
					if (data[off] == '\n') {
						return off;
					}
					continue;
				}
				ColumnLog c = getColumnLog(columns, cIndex++);
				c.setUpdate(true);
				c.setName(data, off, end - off);
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
			for (;;) {
				end = findNextChar(data, off, ':');
				if (data[end + 3] == '1') {
					PrimaryColumnLog c = r.getPrimaryColumn();
//					c.setName(data, off, end - off);
					off = end + I_SKIP;
					end = findNextChar(data, off, '|');
					c.setLongValue(parseLong(data, off, end));
//					c.setValue(data,off,end-off);
					off = end + 1;
					continue;
				}
				ColumnLog c = getColumnLog(columns, cIndex++);
				c.setName(data, off, end - off);
				c.setUpdate(true);
				off = end + I_SKIP;
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
