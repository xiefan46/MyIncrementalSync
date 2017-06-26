package com.alibaba.middleware.race.sync.service;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.model.result.ReadResult;

/**
 * Created by xiefan on 6/26/17.
 */
public class ReadStage {

    private Logger logger	= LoggerFactory.getLogger(getClass());

    private ConcurrentLinkedQueue<ReadResult> resultQueue = new ConcurrentLinkedQueue<>();

    private Context context = Context.getInstance();

    private ParseStage parseStage;

    public ReadStage(ParseStage parseStage){
        this.parseStage = parseStage;
    }

    public void start(){
        BlockReaderer r = new BlockReaderer(parseStage);
        new Thread(r).start();
    }

}
