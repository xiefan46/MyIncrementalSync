package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.sync.codec.ByteArray2;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.RecordMap;

public class ParseThread extends WorkThread {

	private static final int	HEAD_SKIP		= "|mysql-bin.".length()+8;

	private static final int	I_ID_SKIP		= "I|id:1:1|NULL|".length();
	
	private static final int	TIME_SKIP		= "1496720884000".length() + 1;

	private static final int	U_D_ID_SKIP		= "U|id:1:1|".length();

	private static final int	U_D_SKIP			= "1:1|X".length();
	
	private final ByteArray2	byteArray2		= new ByteArray2(null, 0, 0);

	private Context				context;
	
	private BlockingQueue<ReadTask>	tasks		= new ArrayBlockingQueue<>(16);

	public ParseThread(Context context, int index) {
		super("parse-", index);
		this.context = context;
		this.setWork(true);
	}

	private int findNextChar(byte[] data, int offset, char c) {
		for (;data[++offset] != c;) {}
		return offset;
	}

	private byte getName(Table table, byte[] bytes, int off, int len) {
		return table.getIndex(byteArray2.reset(bytes, off, len));
	}

	private boolean inRange(int pk, int startId, int endId) {
		return pk > startId && pk < endId;
	}

	public void offerTask(ReadTask task) {
		tasks.offer(task);
	}

	private int parseLong(byte[] data, int offset, int end) {
		int all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}

	protected void work() throws Exception {
		ReadTask task = tasks.poll(16, TimeUnit.MICROSECONDS);
		if (task == null) {
			return;
		}
		if (task == ReadTask.END_TASK) {
			context.getMainThread().setWorkDone();
			return;
		}
		Context context = this.context;
		Table table = context.getTable();
		RecordMap recordMap = context.getRecordMap();
		int tableSchemaLen = table.getTableSchemaLen();
		short version = task.getVersion();
		byte[] data = task.getBuf().array();
		int limit = task.getBuf().limit();
		int off = 0;
		for(;off < limit;){
			int startId = context.getStartId();
			int endId = context.getEndId();
			off = findNextChar(data, off + HEAD_SKIP, '|');
			off = TIME_SKIP + tableSchemaLen + off;
			byte alterType = data[off];
			if (Constants.UPDATE == alterType) {
				off += U_D_ID_SKIP;
				int end = findNextChar(data, off, '|');
				int beforePk = parseLong(data, off, end);
				off = end + 1;
				end = findNextChar(data, off, '|');
				int pk = parseLong(data, off, end);
				off = end + 1;
				if (beforePk != pk) {
					if (inRange(beforePk, startId, endId)) {
						recordMap.powerDecrement(beforePk);
					}
					off = findNextChar(data, end, '\n') + 1;
					continue;
				} else {
					if (!inRange(pk, startId, endId)) {
						off = findNextChar(data, end + 12, '\n') + 1;
						continue;
					}
				}
				recordMap.lockRecord(pk);
				for (;;) {
					end = findNextChar(data, off, ':');
					byte name = getName(table, data, off, end - off);
					off = end + U_D_SKIP;
					end = findNextChar(data, off, '|');
					off = end + 1;
					end = findNextChar(data, off, '|');
					recordMap.setColumn(pk, name, version, data, off, end - off);
					off = end + 1;
					if (data[off] == '\n') {
						recordMap.releaseRecordLock(pk);
						off++;
						break;
					}
				}
				continue;
			}

			if (Constants.DELETE == alterType) {
				off += U_D_ID_SKIP;
				int end = findNextChar(data, off, '|');
				int pk = parseLong(data, off, end);
				if (inRange(pk, startId, endId)) {
					recordMap.powerDecrement(pk);
				}
				off = findNextChar(data, end + table.getDelSkip(), '\n') + 1;
				continue;
			}

			if (Constants.INSERT == alterType) {
				off += I_ID_SKIP;
				int end = findNextChar(data, off, '|');
				int pk = parseLong(data, off, end);
				if (!inRange(pk, startId, endId)) {
					off = findNextChar(data, end + table.getDelSkip(), '\n') + 1;
					continue;
				}
				int[] colsSkip = table.getColumnNameSkip();
				recordMap.powerIncrement(pk);
				recordMap.lockRecord(pk);
				off = end + 1;
				byte cIndex = 0;
				for (;;) {
					off += colsSkip[cIndex];
					end = findNextChar(data, off, '|');
					recordMap.setColumn(pk, cIndex++, version, data, off, end - off);
					off = end + 1;
					if (data[off] == '\n') {
						recordMap.releaseRecordLock(pk);
						off++;
						break;
					}
				}
				continue;
			}
			throw new RuntimeException(String.valueOf(alterType));
		}
		context.getByteBufPool().free(task);
	}

}
