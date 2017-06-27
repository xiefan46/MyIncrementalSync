package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.RecordMap;

/**
 * @author wangkai
 */
public class RecordLogCodec3 {

	private final int				U_D_SKIP		= "1:1|X".length();

	private final int				I_ID_SKIP	= "I|id:1:1|NULL|".length();

	private final int				HEAD_SKIP		= "|mysql-bin.".length() + 5;

	private final int				TIME_SKIP		= "1496720884000".length() + 1;
	
	private final int				U_D_ID_SKIP		= "U|id:1:1|".length();

	private final ByteArray2			byteArray2	= new ByteArray2(null, 0, 0);

	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	public int decode(Context context, int v, byte[] data, byte[] tableSchema, int offset) {
		Table table = context.getTable();
		RecordMap recordMap = context.getRecordMap();
		int startId = context.getStartId();
		int endId = context.getEndId();
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
		off += TIME_SKIP;
		//		if (!compare(data, off + 1, tableSchema)) {
		//			return findNextChar(data, offset + tableSchema.length, '\n');
		//		}
		int end;
		off = off + tableSchema.length + 2;
		byte alterType = data[off];
		if (Constants.UPDATE == alterType) {
			off += U_D_ID_SKIP;
			end = findNextChar(data, off, '|');
			int beforePk = parseLong(data, off, end);
			off = end + 1;
			end = findNextChar(data, off, '|');
			int pk = parseLong(data, off, end);
			off = end + 1;
			if (beforePk != pk) {
				if (inRange(beforePk, startId, endId)) {
					recordMap.get(beforePk).powerDecrement();
				}
				return findNextChar(data, end, '\n');
			} else {
				if (!inRange(pk, startId, endId)) {
					return findNextChar(data, end, '\n');
				}
			}
			Record record = recordMap.get(pk);
			record.lockRecord();
			for (;;) {
				end = findNextChar(data, off, ':');
				byte name = getName(table, data, off, end - off);
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				record.setColumn(name, v, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					record.releaseRecordLock();
					return off;
				}
			}
		}

		if (Constants.DELETE == alterType) {
			off += U_D_ID_SKIP;
			end = findNextChar(data, off, '|');
			int pk = parseLong(data, off, end);
			if (inRange(pk, startId, endId)) {
				recordMap.get(pk).powerDecrement();
			}
			off = end + table.getDelSkip();
			return findNextChar(data, off, '\n');
		}

		if (Constants.INSERT == alterType) {
			off += I_ID_SKIP;
			end = findNextChar(data, off, '|');
			int pk = parseLong(data, off, end);
			if (!inRange(pk, startId, endId)) {
				return findNextChar(data, end + table.getDelSkip(), '\n');
			}
			int [] colsSkip = table.getColumnNameSkip();
			Record record = recordMap.get(pk);
			record.powerIncrement();
			record.lockRecord();
			off = end + 1;
			byte cIndex = 0;
			for (;;) {
				off += colsSkip[cIndex];
				end = findNextChar(data, off, '|');
				record.setColumn(cIndex++, v, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					record.releaseRecordLock();
					return off;
				}
			}
		}
		throw new RuntimeException(String.valueOf(alterType));
	}
	
	private boolean inRange(int pk,int startId,int endId){
		return pk > startId && pk < endId;
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
