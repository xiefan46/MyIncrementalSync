package com.alibaba.middleware.race.sync.stage;

import com.alibaba.middleware.race.sync.RangeSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.model.result.ParseResult;

import static com.alibaba.middleware.race.sync.Config.CALCULATOR_COUNT;

/**
 * Created by xiefan on 6/24/17.
 */
public class CalculateStage implements Constants {

	private static Logger	logger			= LoggerFactory.getLogger(CalculateStage.class);

	private Calculator[]	calculators		= new Calculator[CALCULATOR_COUNT];

	private Thread[]		threads			= new Thread[CALCULATOR_COUNT];

	private Context		context			= Context.getInstance();

	private byte[]			oneCol			= new byte[8];

	private RangeSearcher	searcher;

	private MergeStage		mergeStage;

	public CalculateStage(MergeStage mergeStage) {
		this.mergeStage = mergeStage;
		searcher = Context.getInstance().getRangeSearcher();
	}

	public void submit(int threadId, ParseResult result) {
		calculators[threadId].submit(result);
	}

	public void start() {
		long start = System.currentTimeMillis();
		for (int i = 0; i < CALCULATOR_COUNT; i++) {
			calculators[i] = new Calculator(i, mergeStage);
			threads[i] = new Thread(calculators[i]);
			threads[i].start();
		}
		logger.info("CalculateStage start cost time : {}", System.currentTimeMillis() - start);
	}

	public void waitForOk() throws InterruptedException {
		for (int i = 0; i < CALCULATOR_COUNT; i++) {
			threads[i].join();
		}
	}

}
