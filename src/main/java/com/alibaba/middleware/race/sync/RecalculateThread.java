package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Dispatcher.Task;
import com.alibaba.middleware.race.sync.model.Node;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.carrotsearch.hppc.IntObjectHashMap;

/**
 * Created by xiefan on 6/16/17.
 */
public class RecalculateThread extends WorkThread implements Constants{
	
	private static Logger logger = LoggerFactory.getLogger(RecalculateThread.class);
	
	private IntObjectHashMap<byte []>		records;

	private Table					table;

	private Task					task;
	
	private MainThread				mainThread;
	
	public RecalculateThread(Context context, IntObjectHashMap<byte []> records,Task task,int i) {
		super("recal-",i);
		this.table = context.getTable();
		this.records = records;
		this.task = task;
		this.mainThread = context.getMainThread();
	}

	/* (non-Javadoc)
	 * @see com.alibaba.middleware.race.sync.WorkThread#work()
	 */
	@Override
	protected void work() throws Exception {
		IntObjectHashMap<byte []> records = this.records;
		Table table = this.table;
		Node<RecordLog> cnr = task.rootNode.getNext();
		int limit = task.limit;
		for (int i = 0; i < limit; i++) {
			RecordLog r = cnr.getValue();
			if (r.getPk() < 0) {
				logger.info("pk < 0,{}",r.getPk());
				cnr = cnr.getNext();
				continue;
			}
			received(table, records, cnr.getValue());
			cnr = cnr.getNext();
		}
		setWork(false);
		mainThread.recalDone(getIndex());
	}

	public IntObjectHashMap<byte []> getRecords() {
		return records;
	}

	public void received(Table table,IntObjectHashMap<byte []> records, RecordLog r) throws Exception {
		int pk = r.getPk();
		switch (r.getAlterType()) {
		case Constants.UPDATE:
			update(records.get(pk), r);
			break;
		case Constants.PK_UPDATE:
			int beforeValue = r.getBeforePk();
			byte[] oldRecord = records.remove(beforeValue);
			update(oldRecord, r);
			records.put(pk, oldRecord);
			break;
		case Constants.DELETE:
			records.remove(pk);
			break;
		case Constants.INSERT:
			records.put(pk, update(table.newRecord(), r));
			break;
		default:
			break;
		}
	}

	private byte[] update(byte[] oldRecord, RecordLog recordLog) {
		byte [] cols = recordLog.getColumns();
		for (int i = 0; i < recordLog.getEdit(); i++) {
			int off = i * 8;
			byte name = cols[off++];
			int len = cols[off++];
			RecordLog.setColumn(oldRecord,name, name, cols, off, len);
		}
		return oldRecord;
	}
	
}
