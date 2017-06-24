package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.RecalculateContext;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * @author wangkai
 */
public class RecordLogCodec2 {

	private static RecordLogCodec2	recordLogCodec	= new RecordLogCodec2();

	private final int				U_D_SKIP		= "1:1|X".length();

	private final int				I_SKIP		= "1:1|NULL|X".length();

	private final int				HEAD_SKIP		= "|mysql-bin.".length() + 0;

	private final int				TIME_SKIP		= "1496720884000".length() + 1;

	private final ByteArray2			byteArray2	= new ByteArray2(null, 0, 0);

	public static RecordLogCodec2 get() {
		return recordLogCodec;
	}

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

	public int decode(RecalculateContext context, byte[] data, byte[] tableSchema, int offset) {
		Table table = context.getTable();
//		Map<Integer, byte[]> records = context.getRecords();
		IntObjectHashMap<byte[]> records = context.getRecords();
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
		off += TIME_SKIP;
		//		if (!compare(data, off + 1, tableSchema)) {
		//			return findNextChar(data, offset + tableSchema.length, '\n');
		//		}
		int end;
		off = off + tableSchema.length + 2;
		byte alterType = data[off];
		off += 2;
		if (Constants.UPDATE == alterType) {
			byte[] record = null;
			end = findNextChar(data, off, ':');
			if (data[end + 3] == '1') {
				//				c.setName(data, off, end - off);
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				int beforePk = parseLong(data, off, end);
				off = end + 1;
				end = findNextChar(data, off, '|');
				int pk = parseLong(data, off, end);
				//				c.setValue(data,off,end-off);
				off = end + 1;
				if (beforePk != pk) {
					record = records.remove(beforePk);
					records.put(pk, record);
				} else {
					record = records.get(pk);
				}
				if (data[off] == '\n') {
					return off;
				}
			}
			for (;;) {
				end = findNextChar(data, off, ':');
				//				r.increamentEdit();
				byte name = getName(table, data, off, end - off);
				//				System.out.println(new String(c.getNameByte()));
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				RecordLog.setColumn(record, name, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					return off;
				}
			}
		}

		if (Constants.DELETE == alterType) {
			end = findNextChar(data, off, ':');
			//			c.setName(data, off, end - off);
			off = end + U_D_SKIP;
			end = findNextChar(data, off, '|');
			records.remove(parseLong(data, off, end));
			//			c.setValue(data,off,end-off);
			return findNextChar(data, end, '\n');
		}

		if (Constants.INSERT == alterType) {
			byte[] record = table.newRecord();
			end = findNextChar(data, off, ':');
			if (data[end + 3] == '1') {
				//				c.setName(data, off, end - off);
				off = end + I_SKIP;
				end = findNextChar(data, off, '|');
				records.put(parseLong(data, off, end), record);
				//				c.setValue(data,off,end-off);
				off = end + 1;
			}
			byte cIndex = 0;
			for (;;) {
				end = findNextChar(data, off, ':');
				//				System.out.println(new String(c.getNameByte()));
				//				r.increamentEdit();
				off = end + I_SKIP;
				end = findNextChar(data, off, '|');
				RecordLog.setColumn(record, cIndex++, data, off, end - off);
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

}
