package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * Created by xiefan on 6/21/17.
 */
public class AllLog {

	private static final Logger	logger	= LoggerFactory.getLogger(AllLog.class);

	public static final byte		INSERT	= (byte) 0b00000000;
	public static final byte		UPDATE	= (byte) 0b01000000;
	public static final byte		DELETE	= (byte) 0b10000000;
	public static final byte		PK_UPDATE	= (byte) 0b11000000;

	ByteBuf					buf;										//TODO off-heap

	private byte				INSERT_HEADER;

	private int				scanCount	= 0;

	public void init(Table table) {
		long old = System.currentTimeMillis();
		this.buf = UnpooledByteBufAllocator.getHeapInstance()
				.allocate((int) (1.1 * 1024 * 1024 * 1024));
		logger.info("申请1G内存耗时:{}", (System.currentTimeMillis() - old));
		INSERT_HEADER = (byte) (4 + table.getColumnSize());
	}

	public void putRecord(RecordLog r) {
		ByteBuf buf = this.buf;
		byte len;
		switch (r.getAlterType()) {
		case Constants.INSERT:
			buf.putInt(r.getPrimaryColumn().getLongValue());
			len = INSERT_HEADER;
			for (int i = 0; i < r.getEdit(); i++) {
				ColumnLog c = r.getColumn(i);
				byte l = c.getValueLen();
				len += l;
				buf.putByte(l);
				buf.put(c.getValue(), c.getValueOff(), l);
			}
			buf.putByte((byte) (INSERT | len));
			break;
		case Constants.UPDATE:
			buf.putInt(r.getPrimaryColumn().getLongValue());
			len = 4;
			for (int i = 0; i < r.getEdit(); i++) {
				ColumnLog c = r.getColumn(i);
				byte l = c.getValueLen();
				byte h = (byte) ((c.getName() << 4) | l);
				len = (byte) (len + 1 + l);
				buf.putByte(h);
				buf.put(c.getValue(), c.getValueOff(), l);
			}
			buf.putByte((byte) (UPDATE | len));
			break;
		case Constants.PK_UPDATE:
			buf.putInt(r.getPrimaryColumn().getBeforeValue());
			buf.putInt(r.getPrimaryColumn().getLongValue());
			len = 8;
			for (int i = 0; i < r.getEdit(); i++) {
				ColumnLog c = r.getColumn(i);
				byte l = c.getValueLen();
				byte h = (byte) ((c.getName() << 4) | l);
				len = (byte) (len + 1 + l);
				buf.putByte(h);
				buf.put(c.getValue(), c.getValueOff(), l);
			}
			buf.putByte((byte) (PK_UPDATE | len));
			break;
		case Constants.DELETE:
			buf.putInt(r.getPrimaryColumn().getLongValue());
			buf.putByte((byte) (DELETE | 4));
			break;
		}
	}

	public Map<Integer, Record> reverseDeal(int startId, int endId, Table table) {
		Map<Integer, Record> resultMap = new HashMap<>();
		Map<Integer, Record> middleMap = initMiddleMap(startId, endId, table);
		ByteBuf buf = this.buf;
		int end = buf.position();
		int cols = table.getColumnSize();
		while (end != 0) {
			scanCount++;
			byte h = buf.getByte(--end);
			byte alterType = (byte) (h & 0b11000000);
			int len = (h & 0b00111111);
			int pk;
			int off;
			Record r;
			switch (alterType) {
			case INSERT:
				off = end - len;
				buf.position(off);
				buf.limit(end);
				end = off;
				pk = buf.getInt();
				r = middleMap.get(pk);
				if (r != null) {
					if (r.isDelete()) {
						middleMap.remove(pk);
						break;
					}
					for (int i = 0; i < cols; i++) {
						byte l = buf.getByte();
						if (r.getColumn(i) == 0) {
							r.setColumn(buf.position(), l, i);
							r.countDown();
						}
						buf.position(buf.position() + l);
					}
					//遇到insert表示一定结束
					middleMap.remove(pk);
					if (r.getFinalPk() != null) {
						resultMap.put(r.getFinalPk(), r);
					} else {
						resultMap.put(pk, r);
					}

				}
				break;
			case UPDATE:
				off = end - len;
				buf.position(off);
				buf.limit(end);
				end = off;
				pk = buf.getInt();
				r = middleMap.get(pk);
				if (r != null && !r.isDelete()) {
					update(buf, r, middleMap, resultMap, pk);
				}
				break;
			case PK_UPDATE:
				off = end - len;
				buf.position(off);
				buf.limit(end);
				end = off;
				int oldPk = buf.getInt();
				pk = buf.getInt();
				r = middleMap.get(pk);
				if (r != null) {
					if (r.isDelete()) {
						middleMap.remove(pk);
						middleMap.put(oldPk, r);
					} else {
						//middleMap.remove(oldPk);
						if (r.getFinalPk() == null) {
							r.setFinalPk(pk);
						}
						middleMap.remove(pk);
						middleMap.put(oldPk, r);
						update(buf, r, middleMap, resultMap, pk);
					}
				} else {
					middleMap.remove(oldPk);
				}
				break;
			case DELETE:
				off = end - len;
				buf.position(off);
				buf.limit(end);
				end = off;
				pk = buf.getInt();
				r = middleMap.get(pk);
				if (r != null) {
					if (r.isDelete()) {
						throw new RuntimeException("wrong delete");
					}
					r.setAlterType(DELETE);
					r.setColumns(null);
				}
				break;
			default:
				throw new RuntimeException("AlterType not found. type : " + (char) alterType);
			}
		}
		return resultMap;
	}

	private void update(ByteBuf buf, Record r, Map<Integer, Record> middleMap,
			Map<Integer, Record> resultMap, Integer pk) {

		for (; buf.hasRemaining();) {
			byte ch = buf.getByte();
			byte name = (byte) ((ch & 0xff) >> 4);
			byte l = (byte) (ch & 0b00001111);
			if (r.getColumn(name) == 0) {
				r.setColumn(buf.position(), l, name);
				r.countDown();
			}
			buf.position(buf.position() + l + 1);
		}
		if (r.dealOver()) {
			middleMap.remove(pk);
			if (r.getFinalPk() != null) {
				resultMap.put(r.getFinalPk(), r);
			} else {
				resultMap.put(pk, r);
			}
			/*
			 * if (resultCount == 0) { logger.info("reverse 提前结束. 扫描条目 : " +
			 * scanCount); return resultMap; }
			 */
		}

	}

	public Map<Integer, Record> initMiddleMap(int startId, int endId, Table table) {
		long start = System.currentTimeMillis();
		Map<Integer, Record> middleMap = new HashMap<>();
		for (int i = startId + 1; i < endId; i++) {
			middleMap.put(i, table.newRecord());
		}
		logger.info("init middleware map cost time : {}", System.currentTimeMillis() - start);
		return middleMap;
	}

	public ByteBuf getBuffer() {
		return buf;
	}

	public int getScanCount() {
		return scanCount;
	}

	public void setScanCount(int scanCount) {
		this.scanCount = scanCount;
	}
}
