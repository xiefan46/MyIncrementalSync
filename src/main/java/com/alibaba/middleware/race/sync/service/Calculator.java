package com.alibaba.middleware.race.sync.service;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.common.RangeSearcher;
import com.alibaba.middleware.race.sync.map.ArrayHashMap2;
import com.alibaba.middleware.race.sync.model.result.ParseResult;
import com.alibaba.middleware.race.sync.model.result.CalculateResult;

/**
 * Created by xiefan on 6/26/17.
 */
public class Calculator implements Runnable, Constants {

	private ConcurrentLinkedQueue<ParseResult>	input		= new ConcurrentLinkedQueue<>();

	private Map<Integer, ParseResult>			waitingMap	= new HashMap<>();

	private ArrayHashMap2					recordMap;

	private int							curBlockId	= 0;

	private RangeSearcher					rangeSearcher	= Context.getInstance()
			.getRangeSearcher();

	private int							id;

	private int							startId;

	private int							endId;

	private MergeStage						mergeStage;

	private byte[]							col			= new byte[8];

	public Calculator(int id, MergeStage mergeStage) {
		this.id = id;
		this.mergeStage = mergeStage;
		this.startId = rangeSearcher.getStartId(id);
		this.endId = rangeSearcher.getEndId(id);
		this.recordMap = new ArrayHashMap2(Context.getInstance().getTable(), startId, endId );
	}

	@Override
	public void run() {
		try {
			boolean stop = false;
			while (!stop || !input.isEmpty()) {
				ParseResult parseResult = input.poll();
				if (parseResult == null) {
					Thread.currentThread().sleep(10);
					continue;
				}
				if (parseResult.getId() == -1) {
					dealTasks();
					stop = true;
				}
				waitingMap.put(parseResult.getId(), parseResult);
				dealTasks();
			}
			mergeStage.submit(new CalculateResult(this.recordMap, id));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void dealTasks() {
		while (waitingMap.containsKey(curBlockId)) {
			ParseResult task = waitingMap.get(curBlockId);
			waitingMap.remove(curBlockId);
			curBlockId++;
			for (ByteBuffer buffer : task.getList()) {
				dealRecord(buffer);
				Context.getInstance().getRecordLogPool().freeBuffer(buffer);
			}
		}
	}

	public void dealRecord(ByteBuffer buffer) {
		while (buffer.hasRemaining()) {
			byte alterType = buffer.get();
			int id;
			switch (alterType) {
			case INSERT:
				id = buffer.getInt();
				recordMap.newRecord(id, buffer);
				break;
			case DELETE:
				id = buffer.getInt();
				recordMap.remove(id);
				break;
			case PK_UPDATE:
				id = buffer.getInt();
				int oldId = buffer.getInt();
				recordMap.remove(oldId);
				break;
			case UPDATE:
				id = buffer.getInt();
				buffer.get(col);
				recordMap.setColumn(id, col[0], col, 0, col[1]);
				break;
			}
		}

	}

	public void submit(ParseResult task) {
		this.input.add(task);
	}

	public ArrayHashMap2 getRecordMap() {
		return recordMap;
	}
}
