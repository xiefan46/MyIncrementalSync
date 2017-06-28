package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.sync.codec.ByteArray2;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.RecordMap;

public class ParseThread extends WorkThread {

	private BlockingQueue<ReadTask>	tasks		= new ArrayBlockingQueue<>(16);

	private Context				context;
	
	public ParseThread(Context context, int index) {
		super("parse-", index);
		this.context = context;
		this.setWork(true);
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
		int tableSchemaLen = table.getTableSchemaLen();
		int version = task.getVersion();
		byte[] readBuffer = task.getBuf().array();
		int limit = task.getBuf().limit();
		int off = 0;
		for(;off < limit;){
			int newOff = decode(context,version, readBuffer, tableSchemaLen, off);
			off = newOff + 1;
		}
		context.getByteBufPool().free(task);
	}

	public void offerTask(ReadTask task) {
		tasks.offer(task);
	}
	
	private static final int	U_D_SKIP			= "1:1|X".length();

	private static final int	I_ID_SKIP		= "I|id:1:1|NULL|".length();

	private static final int	HEAD_SKIP		= "|mysql-bin.".length();

	private static final int	TIME_SKIP		= "1496720884000".length() + 1;

	private static final int	U_D_ID_SKIP		= "U|id:1:1|".length();

	private final ByteArray2	byteArray2		= new ByteArray2(null, 0, 0);

	public int decode(Context context, int v, byte[] data, int tableSchemaLen, int offset) {
		Table table = context.getTable();
		RecordMap recordMap = context.getRecordMap();
		int startId = context.getStartId();
		int endId = context.getEndId();
		int off = findNextChar(data, offset + HEAD_SKIP, '|');
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
				return findNextChar(data, end, '\n');
			} else {
				if (!inRange(pk, startId, endId)) {
					return findNextChar(data, end, '\n');
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
				recordMap.setColumn(pk, name, v, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					recordMap.releaseRecordLock(pk);
					return off;
				}
			}
		}

		if (Constants.DELETE == alterType) {
			off += U_D_ID_SKIP;
			int end = findNextChar(data, off, '|');
			int pk = parseLong(data, off, end);
			if (inRange(pk, startId, endId)) {
				recordMap.powerDecrement(pk);
			}
			off = end + table.getDelSkip();
			return findNextChar(data, off, '\n');
		}

		if (Constants.INSERT == alterType) {
			off += I_ID_SKIP;
			int end = findNextChar(data, off, '|');
			int pk = parseLong(data, off, end);
			if (!inRange(pk, startId, endId)) {
				return findNextChar(data, end + table.getDelSkip(), '\n');
			}
			int[] colsSkip = table.getColumnNameSkip();
			recordMap.powerIncrement(pk);
			recordMap.lockRecord(pk);
			off = end + 1;
			byte cIndex = 0;
			for (;;) {
				off += colsSkip[cIndex];
				end = findNextChar(data, off, '|');
				recordMap.setColumn(pk, cIndex++, v, data, off, end - off);
				off = end + 1;
				if (data[off] == '\n') {
					recordMap.releaseRecordLock(pk);
					return off;
				}
			}
		}
		throw new RuntimeException(String.valueOf(alterType));
	}

	private boolean inRange(int pk, int startId, int endId) {
		return pk > startId && pk < endId;
	}

	private byte getName(Table table, byte[] bytes, int off, int len) {
		return table.getIndex(byteArray2.reset(bytes, off, len));
	}

	private int findNextChar(byte[] data, int offset, char c) {
		for (;data[++offset] != c;) {}
		return offset;
	}

	private int parseLong(byte[] data, int offset, int end) {
		int all = 0;
		for (int i = offset; i < end; i++) {
			all = all * 10 + (data[i] - 48);
		}
		return all;
	}
	
	
	
	
	
	
	
	
	
	

}
