package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.model.Table;
import com.alibaba.middleware.race.sync.util.RecordFactory;
import com.alibaba.middleware.race.sync.util.VFactory;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 */
public class ReaderThread extends Thread {

	private Logger				logger	= LoggerFactory.getLogger(getClass());

	private Context			context;
	
	private Map<Integer, Record> finalRecords;
	
	private RecalculateThread [] recalculateThreads;
	
	private CountDownLatch		countDownLatch;
	
	private VFactory<Record>		vFactory;
	
	public ReaderThread(Context context) {
		this.context = context;
	}
	
	public void init(){
		int threadNum = context.getThreadNum();
		int blockSize = context.getBlockSize();
		recalculateThreads = new RecalculateThread[threadNum];
		vFactory = new RecordFactory(context.getTable().getColumnSize());
		ByteBufAllocator allocator = UnpooledByteBufAllocator.getHeapInstance();
		for (int i = 0; i < threadNum; i++) {
			ByteBuf buf = allocator.allocate(blockSize);
			Map<Integer,Record> records = new HashMap<>((int) (1024 * 1024 * (16f / threadNum)));
			recalculateThreads[i] = new RecalculateThread(this, buf, records, i);
		}
		
		finalRecords = recalculateThreads[0].getRecords();
		
		for (int i = 0; i < threadNum; i++) {
			recalculateThreads[i].start();
		}
	}

	@Override
	public void run() {
		try {
			long startTime = System.currentTimeMillis();
			execute(context, context.getReadChannel());
			logger.info("线程 {} 执行耗时: {}", Thread.currentThread().getId(), System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void execute(Context context,MuiltFileInputStream channel) throws Exception {
		for(;channel.hasRemaining();){
			long startTime = System.currentTimeMillis();
			countDownLatch = new CountDownLatch(context.getThreadNum());
			for(RecalculateThread t : recalculateThreads){
				ByteBuf buf = t.getBuf();
				buf.clear();
				int len = channel.readFull(buf,buf.capacity() - 1024);
				if (len == -1) {
					buf.limit(0);
				}else{
					buf.flip();
				}
				t.setWork(true);
				t.wakeup();
			}
			long time1 = System.currentTimeMillis() - startTime;
			startTime = System.currentTimeMillis();
			startTime = System.currentTimeMillis();
			countDownLatch.await();
			long time2 = System.currentTimeMillis() - startTime;
			startTime = System.currentTimeMillis();
			Table table = context.getTable();
			Map<Integer, Record> finalRecords = this.finalRecords;
			startTime = System.currentTimeMillis();
			for (int i = 1; i < recalculateThreads.length; i++) {
				RecalculateThread t = recalculateThreads[i];
				context.getReceiver().receivedFinal(t.getPool(), table, finalRecords, t.getRecords());
			}
			long time3 = System.currentTimeMillis() - startTime;
			startTime = System.currentTimeMillis();
			logger.info("读取完成:{}，处理完成:{},合并完成:{}",time1,time2,time3);
		}
		for(RecalculateThread t : recalculateThreads){
			t.setRunning(false);
		}
	}

	public Context getContext() {
		return context;
	}
	
	public void done(int index){
		countDownLatch.countDown();
	}

	public long[] getRecord(int id) {
		return finalRecords.get(id).getColumns();
	}
	
	public VFactory<Record> getVFactory() {
		return vFactory;
	}
	
}
