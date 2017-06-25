package com.alibaba.middleware.race.sync.service;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.sync.model.Block;

/**
 * Created by xiefan on 6/26/17.
 */
public class ParseStage {

	private static final int				PARSER_NUM	= 8;

	private ParseThread[]				parsers		= new ParseThread[PARSER_NUM];

	private Thread[]					threads		= new Thread[PARSER_NUM];

	private DataReplayService			inRangeReplayService;

	private DataReplayService			outRangeReplayService;

	private ConcurrentLinkedQueue<Block>	input;

	public ParseStage(DataReplayService inRangeReplayService,
			DataReplayService outRangeReplayService, ConcurrentLinkedQueue<Block> input) {
		this.inRangeReplayService = inRangeReplayService;
		this.outRangeReplayService = outRangeReplayService;
		this.input = input;
	}

	public void start() {
		for (int i = 0; i < parsers.length; i++) {
			parsers[i] = new ParseThread(input, inRangeReplayService, outRangeReplayService);
			threads[i] = new Thread(parsers[i]);
			threads[i].start();
		}
	}

	public void stop() throws InterruptedException {
		for (int i = 0; i < parsers.length; i++) {
			parsers[i].stop();
			threads[i].join();
		}
	}

}
