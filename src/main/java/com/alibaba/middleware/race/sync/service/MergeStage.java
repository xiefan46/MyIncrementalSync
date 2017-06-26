package com.alibaba.middleware.race.sync.service;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.common.RangeSearcher;
import com.alibaba.middleware.race.sync.map.ArrayHashMap;
import com.alibaba.middleware.race.sync.map.ArrayHashMap2;
import com.alibaba.middleware.race.sync.model.result.CalculateResult;
import com.alibaba.middleware.race.sync.util.ByteArrayBuffer;
import com.alibaba.middleware.race.sync.util.RecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by xiefan on 6/26/17.
 */
public class MergeStage {

	ByteArrayBuffer			byteArrayBuffer;

	private static final Logger	logger		= LoggerFactory.getLogger(MergeStage.class);

	private ArrayHashMap2[]		recordMaps	= new ArrayHashMap2[CalculateStage.CALCULATOR_COUNT];

	public MergeStage() {

	}

	public void start() throws IOException {
		long startTime = System.currentTimeMillis();
		this.byteArrayBuffer = new ByteArrayBuffer(Constants.RESULT_LENGTH + 4, 4);
		for (int i = 0; i < recordMaps.length; i++) {
			writeToBuffer(i, recordMaps[i], this.byteArrayBuffer);
		}
		Context.getInstance().setResultBuffer(this.byteArrayBuffer);
		logger.info("Merge stage cost time : {}", System.currentTimeMillis() - startTime);
	}

	private void writeToBuffer(int id, ArrayHashMap2 recordMap, OutputStream buffer)
			throws IOException {
		RangeSearcher rangeSearcher = Context.getInstance().getRangeSearcher();
		int startId = rangeSearcher.getStartId(id);
		int endId = rangeSearcher.getEndId(id);
		int cols = Context.getInstance().getTable().getColumnSize();
		ByteBuffer array = ByteBuffer.allocate(1024 * 1024 * 1);
		ArrayHashMap resultMap = null;
		if (endId >= resultMap.MAX_NUMBER) {
			throw new RuntimeException("数组太小");
		}
		for (int i = startId; i < endId; i++) {
			byte[] r = recordMap.getRecord(i);
			if (r == null || r[0] != (byte) 1) {
				continue;
			}
			RecordUtil.formatResultString(cols, i, r, array);
			buffer.write(array.array(), 0, array.position());
		}
	}

	public ByteArrayBuffer getResult() {
		return byteArrayBuffer;
	}

	public void submit(CalculateResult calculateResult) {
		recordMaps[calculateResult.getThreadId()] = calculateResult.getRecordMap();
	}
}
