package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.log.DeleteLog;
import com.alibaba.middleware.race.sync.log.InsertLog;
import com.alibaba.middleware.race.sync.log.NumberUpdate;
import com.alibaba.middleware.race.sync.log.PKUpdate;
import com.alibaba.middleware.race.sync.log.StringUpdate;
import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.NumberColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.StringColumnLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiefan on 6/21/17.
 */
public class AllLog implements Constants {

	private static final Logger	logger	= LoggerFactory.getLogger(AllLog.class);

	ByteBuffer				buffer;									//todo off-heap

	private int				scanCount	= 0;

	public AllLog() {
		this.buffer = ByteBuffer.allocate((int) (1.5 * 1024 * 1024 * 1024));
	}

	public void putRecord(RecordLog r) {
		switch (r.getAlterType()) {
		case Constants.INSERT:
			buffer.putInt(r.getPrimaryColumn().getLongValue());
			for (ColumnLog columnLog : r.getColumns()) {
				if (columnLog.isNumberCol()) {
					buffer.putInt(((NumberColumnLog) columnLog).getValue());

				} else {
					buffer.putShort(((StringColumnLog) columnLog).getId());

				}
			}
			buffer.put(INSERT);
			break;
		case Constants.UPDATE:
			if (r.isPKUpdate()) {
				buffer.putInt(r.getPrimaryColumn().getLongValue());
				buffer.putInt(r.getPrimaryColumn().getBeforeValue());
				buffer.put(Constants.PK_UPDATE);

			}
			for (ColumnLog columnLog : r.getColumns()) {
				if (columnLog.isNumberCol()) {
					NumberColumnLog numberColumnLog = (NumberColumnLog) columnLog;
					buffer.putInt(r.getPrimaryColumn().getLongValue());
					buffer.put(numberColumnLog.getNameIndex());
					buffer.putInt(numberColumnLog.getValue());
					buffer.put(Constants.NUMBER_UPDATE);

				} else {
					StringColumnLog stringColumnLog = (StringColumnLog) columnLog;
					buffer.putInt(r.getPrimaryColumn().getLongValue());
					buffer.put(stringColumnLog.getNameIndex());
					buffer.putShort(stringColumnLog.getId());
					buffer.put(Constants.STR_UPDATE);
				}
			}
			break;
		case Constants.DELETE:
			buffer.putInt(r.getPrimaryColumn().getLongValue());
			buffer.put(Constants.DELETE);
			break;
		}
	}

	public Map<Integer, Record> reverseDeal(int startId, int endId, Table table) {
		Map<Integer, Record> resultMap = new HashMap<>();
		Map<Integer, Record> middleMap = initMiddleMap(startId, endId, table);
		int end = buffer.position();
		int resultCount = middleMap.size();
		while (end != 0) {
			scanCount++;
			//System.out.println("end : " + end);
			buffer.position(end - 1);
			byte alterType = buffer.get();
			int pk;
			Record r;
			switch (alterType) {
			case INSERT:
				buffer.position(end - InsertLog.LEN);
				pk = buffer.getInt();
				r = middleMap.get(pk);
				if (r != null) {
					if (r.getAlterType() == DELETE) {
						middleMap.remove(pk);
					} else {
						for (int i = 0; i < table.getStrColSize(); i++) {
							Short value = r.getStrCols()[i];
							Short oldValue = buffer.getShort();
							if (value == null) {
								r.getStrCols()[i] = oldValue;
								r.countDown();
							}

						}
						for (int i = 0; i < table.getNumberColSize(); i++) {
							Integer value = r.getNumberCols()[i];
							Integer oldValue = buffer.getInt();
							if (value == null) {
								r.getNumberCols()[i] = oldValue;
								r.countDown();
							}
						}
						//遇到insert表示一定结束
						middleMap.remove(pk);
						if (r.getFinalPk() != null) {
							resultMap.put(r.getFinalPk(), r);
						} else {
							resultMap.put(pk, r);
						}
						resultCount--;
						/*
						 * if (resultCount == 0) {
						 * logger.info("reverse 提前结束. 扫描条目 : " +
						 * scanCount); return resultMap; }
						 */

					}

				}
				end -= InsertLog.LEN;
				break;
			case STR_UPDATE:
				buffer.position(end - StringUpdate.LEN);
				pk = buffer.getInt();
				r = middleMap.get(pk);
				if (r != null && r.getAlterType() != DELETE) {
					byte index = buffer.get();
					if (r.getStrCols()[index] == null) {
						r.getStrCols()[index] = buffer.getShort();
						r.countDown();
						if (r.dealOver()) {
							resultCount--;
							middleMap.remove(pk);
							if (r.getFinalPk() != null) {
								resultMap.put(r.getFinalPk(), r);
							} else {
								resultMap.put(pk, r);
							}
							resultCount--;
							/*
							 * if (resultCount == 0) {
							 * logger.info("reverse 提前结束. 扫描条目 : " +
							 * scanCount); return resultMap; }
							 */
						}
					}
				}
				end -= StringUpdate.LEN;
				break;
			case NUMBER_UPDATE:
				buffer.position(end - NumberUpdate.LEN);
				pk = buffer.getInt();
				r = middleMap.get(pk);
				if (r != null && r.getAlterType() != DELETE) {
					int index = buffer.get();
					if (r.getNumberCols()[index] == null) {
						r.getNumberCols()[index] = buffer.getInt();
						r.countDown();
						if (r.dealOver()) {
							middleMap.remove(pk);
							if (r.getFinalPk() != null) {
								resultMap.put(r.getFinalPk(), r);
							} else {
								resultMap.put(pk, r);
							}
							resultCount--;
							/*
							 * if (resultCount == 0) {
							 * logger.info("reverse 提前结束. 扫描条目 : " +
							 * scanCount); return resultMap; }
							 */
						}
					}
				}
				end -= NumberUpdate.LEN;
				break;
			case PK_UPDATE:
				buffer.position(end - PKUpdate.LEN);
				pk = buffer.getInt();
				int oldPk = buffer.getInt();
				r = middleMap.get(pk);
				Record r2 = middleMap.remove(oldPk);
				/*
				 * if(r != null && r2 != null){ throw new
				 * RuntimeException("can not handle"); }else if (r != null)
				 * { middleMap.remove(pk); middleMap.put(oldPk, r); if
				 * (r.getFinalPk() == null && r.getAlterType() != DELETE) {
				 * r.setFinalPk(pk); } }else if(r2 != null){
				 * 
				 * }
				 */
				if (r != null) {
					middleMap.remove(pk);
					middleMap.put(oldPk, r);
					if (r.getFinalPk() == null && r.getAlterType() != DELETE) {
						r.setFinalPk(pk);
					}
				}

				end -= PKUpdate.LEN;
				break;
			case DELETE:
				buffer.position(end - DeleteLog.LEN);
				pk = buffer.getInt();
				r = middleMap.get(pk);
				if (r != null) {
					if (r.getAlterType() == DELETE) {
						throw new RuntimeException("wrong delete");
					}
					r.setAlterType(DELETE);
					r.setNumberCols(null);
					r.setStrCols(null);
					/*
					 * resultCount--; if (resultCount == 0) {
					 * logger.info("reverse 提前结束. 扫描条目 : " + scanCount);
					 * return resultMap; }
					 */
				}

				end -= DeleteLog.LEN;

				break;
			default:
				throw new RuntimeException("AlterType not found. type : " + (char) alterType);
			}
		}
		return resultMap;
	}

	public Map<Integer, Record> initMiddleMap(int startId, int endId, Table table) {
		Map<Integer, Record> middleMap = new HashMap<>();
		for (int i = startId + 1; i < endId; i++) {
			middleMap.put(i, table.newRecord());
		}
		return middleMap;
	}

	private boolean in(int pk, int startId, int endId) {
		return pk > startId && pk < endId;
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	public void setBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	public int getScanCount() {
		return scanCount;
	}

	public void setScanCount(int scanCount) {
		this.scanCount = scanCount;
	}
}
