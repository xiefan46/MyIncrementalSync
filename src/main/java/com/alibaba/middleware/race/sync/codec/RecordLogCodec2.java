package com.alibaba.middleware.race.sync.codec;

import java.util.List;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Constants;
import com.alibaba.middleware.race.sync.model.NumberColumnLog;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.StringColumnLog;
import com.alibaba.middleware.race.sync.model.Table;

/**
 * @author wangkai
 */
public class RecordLogCodec2 {

	private static RecordLogCodec2	recordLogCodec	= new RecordLogCodec2();

	private final int				U_D_SKIP		= "1:1|X".length();

	private final int				I_SKIP		= "1:1|NULL|X".length();

	private final int				HEAD_SKIP		= "|mysql-bin.".length();

	private final int				TIME_SKIP		= "1496720884000".length() + 1;

	public static RecordLogCodec2 get() {
		return recordLogCodec;
	}

	private RecordLogCodec2() {
	}

	private Table		table	= new Table();

	private boolean	tableInit	= false;

	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	private ColumnLog getColumnLog(List<ColumnLog> cols, int index) {
		if (index >= cols.size()) {
			cols.add(new ColumnLog());
		}
		return cols.get(index);
	}

	public int decode(byte[] data, byte[] tableSchema, int offset, RecordLog r) {
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
		columns.clear();
		if (Constants.UPDATE == alterType) {
			for (;;) {
				end = findNextChar(data, off, ':');
				if (data[end + 3] == '1') {
					PrimaryColumnLog c = r.getPrimaryColumn();
					//					c.setName(data, off, end - off);
					off = end + U_D_SKIP;
					end = findNextChar(data, off, '|');
					c.setBeforeValue(parseInt(data, off, end));
					off = end + 1;
					end = findNextChar(data, off, '|');
					c.setLongValue(parseInt(data, off, end));
					//					c.setValue(data,off,end-off);
					off = end + 1;
					if (data[off] == '\n') {
						return off;
					}
					continue;
				}
				ColumnLog c;
				if (data[end + 1] == '1') { //number col
					NumberColumnLog numberColumnLog = new NumberColumnLog();
					numberColumnLog
							.setNameIndex(table.getColNameId(data, off, end - off, true));
					numberColumnLog.setNumberCol(true);
					off = end + U_D_SKIP;
					end = findNextChar(data, off, '|');
					off = end + 1;
					end = findNextChar(data, off, '|');
					numberColumnLog.setValue(parseInt(data, off, end));
					c = numberColumnLog;
				} else {
					StringColumnLog stringColumnLog = new StringColumnLog();
					stringColumnLog
							.setNameIndex(table.getColNameId(data, off, end - off, false));
					stringColumnLog.setNumberCol(false);
					off = end + U_D_SKIP;
					end = findNextChar(data, off, '|');
					off = end + 1;
					end = findNextChar(data, off, '|');
					stringColumnLog.setId(table.getStrColValueId(data, off, end - off));
					c = stringColumnLog;
				}
				columns.add(c);
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
			c.setLongValue(parseInt(data, off, end));
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
					c.setLongValue(parseInt(data, off, end));
					//					c.setValue(data,off,end-off);
					off = end + 1;
					continue;
				}

				ColumnLog c;
				if (data[end + 1] == '1') { //number col
					if (!tableInit) {
						table.addCol(data, off, end - off, true);
					}
					NumberColumnLog numberColumnLog = new NumberColumnLog();
					numberColumnLog
							.setNameIndex(table.getColNameId(data, off, end - off, true));
					numberColumnLog.setNumberCol(true);
					off = end + I_SKIP;
					end = findNextChar(data, off, '|');
					numberColumnLog.setValue(parseInt(data, off, end));
					c = numberColumnLog;
				} else {
					if (!tableInit) {
						table.addCol(data, off, end - off, false);
					}
					StringColumnLog stringColumnLog = new StringColumnLog();
					stringColumnLog
							.setNameIndex(table.getColNameId(data, off, end - off, false));
					stringColumnLog.setNumberCol(false);
					off = end + I_SKIP;
					end = findNextChar(data, off, '|');
					stringColumnLog.setId(table.getStrColValueId(data, off, end - off));
					c = stringColumnLog;
				}
				columns.add(c);
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

	private int parseInt(byte[] data, int offset, int end) {
		int all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}

	public void initialize() {

	}

	public boolean isTableInit() {
		return tableInit;
	}

	public void setTableInit(boolean tableInit) {
		this.tableInit = tableInit;
	}

	public Table getTable() {
		return table;
	}
}
