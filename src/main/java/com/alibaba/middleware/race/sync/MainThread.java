package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.CompoundReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.alibaba.middleware.race.sync.channel.RAFInputStream;
import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.channel.SimpleReadChannel;

/**
 * @author wangkai
 */
public class MainThread {

	private Logger logger = LoggerFactory.getLogger(getClass());

	public void execute(Context context) {
		try {
			execute1(context);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void execute0(Context context) throws Exception {
		long startTime = System.currentTimeMillis();
		File root = new File(Constants.DATA_HOME);
		File[] files = root.listFiles();
		logger.debug("files num : " + files.length);
		if (!root.exists()) {
			throw new FileNotFoundException(root.getAbsolutePath());
		}
		ReadChannel[] channels;
		Thread[] ts;
		ReadRecordLogContext[] contexts;
		if (context.isExecuteByCoreProcesses()) {
			int coreProcesses = Runtime.getRuntime().availableProcessors();
			contexts = new ReadRecordLogContext[coreProcesses];
			ts = new Thread[coreProcesses];
			channels = initChannelsByCoreProcesses(root, coreProcesses);
		} else {
			contexts = new ReadRecordLogContext[files.length];
			ts = new Thread[files.length];
			channels = initChannelsByFiles(root, files.length);
		}
		logger.info("Thread num : {}", ts.length);
		for (int i = 0; i < ts.length; i++) {
			contexts[i] = new ReadRecordLogContext(channels[i], context);
			contexts[i].initialize();
			ts[i] = new Thread(new ReadRecordLogThread(contexts[i]));
		}
		logger.info("MainThread 初始化耗时 : {}", System.currentTimeMillis() - startTime);
		startTime = System.currentTimeMillis();
		for (int i = 0; i < ts.length; i++) {
			ts[i].start();
		}
		for (int i = 0; i < ts.length; i++) {
			ts[i].join();
		}
		logger.info("解析记录耗时 : {}", System.currentTimeMillis() - startTime);

	}
	
	private void execute1(Context context) throws Exception {
		long startTime = System.currentTimeMillis();
		File root = new File(Constants.DATA_HOME);
		if (!root.exists()) {
			throw new FileNotFoundException(root.getAbsolutePath());
		}
		File[] files = root.listFiles();
		logger.debug("files num : " + files.length);
		ReadChannel[] channels = initChannels2(root);
		Thread[] ts = new Thread[2];
		ReadRecordLogContext[] contexts = new ReadRecordLogContext[2];
		logger.info("Thread num : {}", ts.length);
		for (int i = 0; i < ts.length; i++) {
			contexts[i] = new ReadRecordLogContext(channels[i], context);
			contexts[i].initialize();
			ts[i] = new Thread(new ReadRecordLogThread(contexts[i]));
		}
		logger.info("MainThread 初始化耗时 : {}", System.currentTimeMillis() - startTime);
		
		initRecalculateThread(context);
		
		startTime = System.currentTimeMillis();
		for (int i = 0; i < ts.length; i++) {
			ts[i].start();
		}
		for (int i = 0; i < ts.length; i++) {
			ts[i].join();
		}
		logger.info("解析记录耗时 : {}", System.currentTimeMillis() - startTime);

	}
	
	private void initRecalculateThread(Context context) throws InterruptedException{
		int coreProcesses = context.getAvailableProcessors();
		RecalculateContext [] contexts = context.getRecalculateContexts();
		Thread[] ts = new Thread[coreProcesses];
		for (int i = 0; i < ts.length; i++) {
			ts[i] = new Thread(new RecalculateThread(contexts[i]));
		}
		
		for (int i = 0; i < ts.length; i++) {
			ts[i].start();
		}
		for (int i = 0; i < ts.length; i++) {
			ts[i].join();
		}
	}

	private ReadChannel[] initChannelsByFiles(File root, int size) throws IOException {
		File[] files = root.listFiles();
		ReadChannel[] rcs = new ReadChannel[files.length];
		Arrays.sort(files);
		for (int i = 0; i < files.length; i++) {
			File f = files[i];
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			rcs[i] = new SimpleReadChannel(new RAFInputStream(raf), 256 * 1024);
			logger.info("File channel ok. File name : {}. File size : {} B", f.getName(),
					f.length());
		}
		return rcs;
	}

	private ReadChannel[] initChannelsByCoreProcesses(File root, int size) throws IOException {
		return CompoundReadChannelSplitor.split(root, size);
	}
	
	private ReadChannel[] initChannels2(File root) throws IOException {
		return MuiltFileReadChannelSplitor.split(root,1024 * 128);
	}

}
