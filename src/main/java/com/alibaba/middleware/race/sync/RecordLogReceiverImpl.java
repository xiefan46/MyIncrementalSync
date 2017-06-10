package com.alibaba.middleware.race.sync;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.ColumnLog;
import com.alibaba.middleware.race.sync.model.Constants;
import com.alibaba.middleware.race.sync.model.PrimaryColumnLog;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * Created by xiefan on 6/4/17.
 */
public class RecordLogReceiverImpl implements RecordLogReceiver {

	private static final Logger	logger	= LoggerFactory.getLogger(RecordLogReceiverImpl.class);

	int						count	= 0;

	int						count2	= 0;

	private List<Long>			logIdList	= Arrays.asList(621L, 170001L);

	@Override
	public void received(RecalculateContext context, RecordLog recordLog) throws Exception {
		Map<Long, Record> records = context.getRecords();
		PrimaryColumnLog pcl = recordLog.getPrimaryColumn();
		Table table = context.getTable();
		Long pk = pcl.getLongValue();
		switch (recordLog.getAlterType()) {
		case Constants.UPDATE:
			if (pcl.isPkChange()) {
				Long beforeValue = pcl.getBeforeValue();
				Record oldRecord = records.remove(beforeValue);
				update(table, oldRecord, recordLog);
				oldRecord.setColum(0, pcl.getValue());
				records.put(pk, oldRecord);
			} else {
				Record oldRecord = records.get(pk);
				update(table, oldRecord, recordLog);
			}
			break;
		case Constants.DELETE:
			records.remove(pk);
			break;
		case Constants.INSERT:
			Record r = update(table, table.newRecord(), recordLog);
			r.setColum(0, pcl.getValue());
			records.put(pk, r);
			break;
		default:
			if (count < 5) {
				logger.info("错误的指令类型");
				count++;
			}
			break;
		}

	}

	private Record update(Table table, Record oldRecord, RecordLog recordLog) {
		if (count2 < 10 && (logIdList.contains(recordLog.getPrimaryColumn().getLongValue())
				|| logIdList.contains(recordLog.getPrimaryColumn().getBeforeValue()))) {
			StringBuilder sb = new StringBuilder();
			sb.append("col update.");
			sb.append(" old pk : " + recordLog.getPrimaryColumn().getBeforeValue());
			sb.append(" new pk: " + recordLog.getPrimaryColumn().getLongValue());
			for (ColumnLog c : recordLog.getColumns()) {
				byte[] name = c.getName();
				byte[] value = c.getValue();
				sb.append(" col name : " + new String(name, 0, name.length) + "  new value : "
						+ new String(value, 0, value.length));
			}
			logger.info("record log: " + sb.toString());
			count2++;
		}
		for (ColumnLog c : recordLog.getColumns()) {
			oldRecord.setColum(table.getIndex(c.getName()), c.getValue());
		}
		return oldRecord;
	}

}
