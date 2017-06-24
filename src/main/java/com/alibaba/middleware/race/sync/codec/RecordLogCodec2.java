package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.common.OPCode;
import com.alibaba.middleware.race.sync.map.ArrayHashMap;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 */
public class RecordLogCodec2 {

	private static RecordLogCodec2	recordLogCodec	= new RecordLogCodec2();

	private final int				U_D_SKIP		= "1:1|X".length();

	private final int				I_ID_SKIP		= "I|id:1:1|NULL|".length();

	private final int				HEAD_SKIP		= "|mysql-bin.".length() + 5;

	private final int				TIME_SKIP		= "1496720884000".length() + 1;

	private final int				U_D_ID_SKIP	= "U|id:1:1|".length();

	private final ByteArray2			byteArray2	= new ByteArray2(null, 0, 0);

	public static RecordLogCodec2 get() {
		return recordLogCodec;
	}

	//public boolean print;

	private RecordLogCodec2() {
	}

	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	public int decode(Context context, byte[] data, byte[] tableSchema, int offset) {
		//print = false;
		Table table = null;
		//		Map<Integer, byte[]> records = context.getRecords();
		ArrayHashMap resultMap = null;
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
		off += TIME_SKIP;
		//		if (!compare(data, off + 1, tableSchema)) {
		//			return findNextChar(data, offset + tableSchema.length, '\n');
		//		}
		int end;
		off = off + tableSchema.length + 2;
		byte alterType = data[off];
		if (OPCode.UPDATE == alterType) {
			off += U_D_ID_SKIP;
			end = findNextChar(data, off, '|');
			int beforePk = parseLong(data, off, end);
			off = end + 1;
			end = findNextChar(data, off, '|');
			int pk = parseLong(data, off, end);
			/*
			 * if (needPrint(pk) || needPrint(beforePk)) print = true;
			 */
			off = end + 1;
			if (beforePk != pk) {
				resultMap.move(beforePk, pk);
			}
			if (data[off] == '\n') {
				return off;
			}
			for (;;) {
				end = findNextChar(data, off, ':');
				byte name = getName(table, data, off, end - off);
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				//RecordLog.setColumn(record, name, data, off, end - off);
				resultMap.setColumn(pk, name, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					return off;
				}
			}
		}

		if (OPCode.DELETE == alterType) {
			off += U_D_ID_SKIP;
			end = findNextChar(data, off, '|');
			int pk = parseLong(data, off, end);
			//print = needPrint(pk);
			resultMap.remove(pk);
			off = end + table.getDelSkip();
			return findNextChar(data, end, '\n');
		}

		if (OPCode.INSERT == alterType) {
			off += I_ID_SKIP;
			//byte[] record = table.newRecord();
			end = findNextChar(data, off, '|');
			int id = parseLong(data, off, end);
			//print = needPrint(id);
			resultMap.newRecord(id);
			//records.put(parseLong(data, off, end), record);
			int[] colsSkip = table.getColumnNameSkip();
			off = end + 1;
			byte cIndex = 0;
			for (;;) {
				off += colsSkip[cIndex];
				end = findNextChar(data, off, '|');
				//RecordLog.setColumn(record, cIndex++, data, off, end - off);
				resultMap.setColumn(id, cIndex++, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					return off;
				}
			}

		}
		throw new RuntimeException(String.valueOf(alterType));
	}

	private byte getName(Table table, byte[] bytes, int off, int len) {
		return table.getIndex(byteArray2.reset(bytes, off, len));
	}

	private int findNextChar(byte[] data, int offset, char c) {
		for (;;) {
			if (data[++offset] == c) {
				return offset;
			}
		}
	}

	private int parseLong(byte[] data, int offset, int end) {
		int all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}

	private boolean needPrint(int pk) {
		if (pk == 604)
			return true;
		return false;
	}

}
