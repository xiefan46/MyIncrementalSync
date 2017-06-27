package com.alibaba.middleware.race.sync.stage;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.sync.Config;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.result.ParseResult;
import com.alibaba.middleware.race.sync.model.result.ReadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiefan on 6/26/17.
 */
public class ParseStage {

	private ParseThread[]					parsers			= new ParseThread[Config.PARSER_COUNT];

	private Thread[]						threads			= new Thread[Config.PARSER_COUNT];

	private CalculateStage					calculateStage;

	private ConcurrentLinkedQueue<ReadResult>	readResultQueue	= new ConcurrentLinkedQueue<>();

	private static final Logger				logger			= LoggerFactory
			.getLogger(ParseStage.class);

	public ParseStage(CalculateStage calculateStage) {
		this.calculateStage = calculateStage;
	}

	public void start() throws InterruptedException {
		for (int i = 0; i < parsers.length; i++) {
			parsers[i] = new ParseThread(calculateStage, this);
			threads[i] = new Thread(parsers[i]);
			threads[i].start();
		}
		startWaitingThread();
	}

	public void submit(ReadResult readResult) {
		this.readResultQueue.add(readResult);
	}

	public ConcurrentLinkedQueue<ReadResult> getReadResultQueue() {
		return readResultQueue;
	}

	private void startWaitingThread() throws InterruptedException {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					long startTime = System.currentTimeMillis();
					for (int i = 0; i < parsers.length; i++) {
						threads[i].join();
					}
					for (int i = 0; i < Config.CALCULATOR_COUNT; i++) {
						calculateStage.submit(i, new ParseResult(-1,null));
					}
					logger.info("所有Parse thread完成耗时 : {}",
							System.currentTimeMillis() - startTime);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
	}

	public void notifyStop() {
		for (int i = 0; i < parsers.length; i++) {
			parsers[i].stop();
		}
	}

}
