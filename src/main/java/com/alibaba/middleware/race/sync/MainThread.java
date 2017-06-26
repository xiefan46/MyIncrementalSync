package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.service.CalculateStage;
import com.alibaba.middleware.race.sync.service.MergeStage;
import com.alibaba.middleware.race.sync.service.ParseStage;
import com.alibaba.middleware.race.sync.service.ReadStage;

/**
 * @author wangkai
 */
public class MainThread {

	private Logger		logger	= LoggerFactory.getLogger(getClass());

	private Context	context	= Context.getInstance();

	public void start(String[] args) {
		try {
			logger.info("--------------Main thread start-----------");
			long start = System.currentTimeMillis();
			context.initQuery(args[0], args[1], Integer.parseInt(args[2]),
					Integer.parseInt(args[3]));
			logger.info("init context cost time : {}", System.currentTimeMillis() - start);
			start = System.currentTimeMillis();
			execute();
			logger.info("Run all thread cost time : {}", System.currentTimeMillis() - start);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void execute() throws Exception {
		MergeStage mergeStage = new MergeStage();
		CalculateStage calculateStage = new CalculateStage(mergeStage);
		ParseStage parseStage = new ParseStage(calculateStage);
		ReadStage readStage = new ReadStage(parseStage);

		long start = System.currentTimeMillis();
		calculateStage.start();
		parseStage.start();
		readStage.start();
		logger.info("start all the stage cost time : {}", System.currentTimeMillis() - start);
		start = System.currentTimeMillis();
		calculateStage.waitForOk();
		logger.info("wait for calculate stage ok cost time : {}",
				System.currentTimeMillis() - start);
		start = System.currentTimeMillis();
		mergeStage.start();
		logger.info("merge result cost time : {}", System.currentTimeMillis() - start);
	}

}
