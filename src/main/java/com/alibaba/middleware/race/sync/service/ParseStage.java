package com.alibaba.middleware.race.sync.service;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.sync.model.result.ParseResult;
import com.alibaba.middleware.race.sync.model.result.ReadResult;

/**
 * Created by xiefan on 6/26/17.
 */
public class ParseStage {

	public static final int					PARSER_NUM		= 8;

	private ParseThread[]					parsers			= new ParseThread[PARSER_NUM];

	private Thread[]						threads			= new Thread[PARSER_NUM];

	private CalculateStage					calculateStage;

	private ConcurrentLinkedQueue<ReadResult>	readResultQueue	= new ConcurrentLinkedQueue<>();

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
					for (int i = 0; i < parsers.length; i++) {
						threads[i].join();
					}
					for (int i = 0; i < CalculateStage.REPLAYER_COUNT; i++) {
						calculateStage.submit(i, new ParseResult( -1));
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();
	}

}
