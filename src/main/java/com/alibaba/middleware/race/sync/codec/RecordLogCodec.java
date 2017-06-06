package com.alibaba.middleware.race.sync.codec;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.PrimaryColumn;
import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 */
public class RecordLogCodec {

	private static RecordLogCodec	recordLogCodec	= new RecordLogCodec();
	
	private final int			U_D_SKIP		= "1:1|X".length();
	
	private final int			I_SKIP		= "1:1|NULL|X".length();

	public static RecordLogCodec get() {
		return recordLogCodec;
	}
	
	private RecordLogCodec(){}

	public Record decode(byte[] data, int offset, int last, long startId, long endId) {
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
		if (Record.UPDATE == r.getAlterType()) {
			r.newColumns();
			for (;;) {
				end = findNextChar(data, off, ':');
				if (data[end + 3] == '1') {
					PrimaryColumn c = new PrimaryColumn();
					r.setPrimaryColumn(c);
					c.setPrimary(true);
					c.setName(new String(data, off, end - off));
					boolean isNumber = data[end + 1] == '1';
					off = end + U_D_SKIP;
					end = findNextChar(data, off, '|');
					if (isNumber) {
						c.setNumber(true);
						c.setBeforeValue(parseLong(data, off, end));
						off = end + 1;
						end = findNextChar(data, off, '|');
						c.setValue(parseLong(data, off, end));
					} else {
						c.setBeforeValue(new String(data, off, end - off));
						off = end + 1;
						end = findNextChar(data, off, '|');
						c.setValue(new String(data, off, end - off));
					}
					if (!selected((long) c.getBeforeValue(), startId, endId)
							|| !selected((long) c.getValue(), startId, endId)) {
						return null;
					}
					off = end + 1;
					if (off == last) {
						return r;
					}
					continue;
				}
				Column c = new Column();
				c.setName(new String(data, off, end - off));
				r.addColumn(c);
				boolean isNumber = data[end + 1] == '1';
				off = end + U_D_SKIP;
				end = findNextChar(data, off, '|');
				off = end + 1;
				end = findNextChar(data, off, '|');
				if (isNumber) {
					c.setNumber(true);
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

		if (Record.DELETE == r.getAlterType()) {
			end = findNextChar(data, off, ':');
			PrimaryColumn c = new PrimaryColumn();
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
			off = end + U_D_SKIP;
			end = findNextChar(data, off, '|');
			if (isNumber) {
				c.setNumber(true);
				c.setValue(parseLong(data, off, end));
			} else {
				c.setValue(new String(data, off, end - off));
			}
			if (!selected((long) c.getValue(), startId, endId)) {
				return null;
			}
			return r;
		}

		if (Record.INSERT == r.getAlterType()) {
			r.newColumns();
			for (;;) {
				end = findNextChar(data, off, ':');
				if (data[end + 3] == '1') {
					PrimaryColumn c = new PrimaryColumn();
					r.setPrimaryColumn(c);
					c.setName(new String(data, off, end - off));
					c.setPrimary(true);
					boolean isNumber = data[end + 1] == '1';
					off = end + I_SKIP;
					end = findNextChar(data, off, '|');
					if (isNumber) {
						c.setNumber(true);
						c.setValue(parseLong(data, off, end));
					} else {
						c.setValue(new String(data, off, end - off));
					}
					//logger.debug("c value : " + c.getValue().toString());
					if (!selected((long) c.getValue(), startId, endId)) {
						return null;
					}
					off = end + 1;
					if (off == last) {
						return r;
					}
					continue;
				}
				Column c = new Column();
				c.setName(new String(data, off, end - off));
				r.addColumn(c);
				boolean isNumber = data[end + 1] == '1';
				off = end + I_SKIP;
				end = findNextChar(data, off, '|');
				if (isNumber) {
					c.setNumber(true);
					c.setValue(parseLong(data, off, end));
				} else {
					c.setValue(new String(data, off, end - off));
				}
				off = end + 1;
				if (off >= last) {
					return r;
				}
			}

		}
		return r;
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

	private boolean selected(long id, long startId, long endId) {
		return id > startId && id < endId;
	}

	public void initialize() {

	}

}
