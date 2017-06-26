package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 */
public class RecordLogCodec2 {

	private final int				U_D_SKIP		= "1:1|X".length();

	private final int				I_ID_SKIP		= "I|id:1:1|NULL|".length();

	private final int				HEAD_SKIP		= "|mysql-bin.".length();

	private final int				TIME_SKIP		= "1496720884000".length() + 1;

	private final int				U_D_ID_SKIP	= "U|id:1:1|".length();

	private final ByteArray2		byteArray2	= new ByteArray2(null, 0, 0);

	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	public int decode(Table table, byte[] data, byte[] tableSchema, int offset, RecordLog r
			,int startId,int endId) {
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
		off += TIME_SKIP;
		//		if (!compare(data, off + 1, tableSchema)) {
		//			return findNextChar(data, offset + tableSchema.length, '\n');
		//		}
		int end;
		off = off + tableSchema.length + 2;
		byte alterType = data[off];
		r.setAlterType(alterType);
		if (Constants.UPDATE == alterType) {
			off += U_D_ID_SKIP;
			end = findNextChar(data, off, '|');
			r.setBeforePk(parseLong(data, off, end));
			off = end + 1;
			end = findNextChar(data, off, '|');
			r.setPk(parseLong(data, off, end));
			off = end + 1;
			if (r.isPkUpdate4Codec()) {
				if (inRange(r.getBeforePk(), startId, endId)) {
					r.setAlterType(Constants.DELETE);
					r.setPk(r.getBeforePk());
					r.setRead(true);
				}
				return findNextChar(data, end, '\n');
			}else{
				if (!inRange(r.getPk(), startId, endId)) {
					return findNextChar(data, end, '\n');
				}
			}
			r.setRead(true);
			if (data[off] == '\n') {
				return off;
			}
			for (;;) {
				byte c = r.getColumn();
				end = findNextChar(data, off, ':');
				byte name = getName(table, data, off, end - off);
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				r.setColumn(c,name, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					return off;
				}
			}
		}

		if (Constants.DELETE == alterType) {
			off += U_D_ID_SKIP;
			end = findNextChar(data, off, '|');
			r.setPk(parseLong(data, off, end));
			off = end + table.getDelSkip();
			if (inRange(r.getPk(), startId, endId)) {
				r.setRead(true);
			}
			return findNextChar(data, off, '\n');
		}

		if (Constants.INSERT == alterType) {
			off += I_ID_SKIP;
			end = findNextChar(data, off, '|');
			r.setPk(parseLong(data, off, end));
			if (!inRange(r.getPk(), startId, endId)) {
				return findNextChar(data, end + table.getDelSkip(), '\n');
			}
			r.setRead(true);
			int[] colsSkip = table.getColumnNameSkip();
			off = end + 1;
			for (;;) {
				byte c = r.getColumn();
				off += colsSkip[c];
				end = findNextChar(data, off, '|');
				r.setColumn(c,c, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
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
