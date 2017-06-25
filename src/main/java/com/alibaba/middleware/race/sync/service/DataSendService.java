package com.alibaba.middleware.race.sync.service;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.entity.SendTask;
import com.alibaba.middleware.race.sync.map.ArrayRecordMap;
import com.alibaba.middleware.race.sync.map.RecordMap;
import com.alibaba.middleware.race.sync.util.Timer;

/**
 * Created by xiefan on 6/24/17.
 */
public class DataSendService {
	private static Logger				stat		= LoggerFactory.getLogger("stat");
	private ConcurrentLinkedQueue<SendTask>	input;
	private Context					context	= Context.getInstance();
	private final long					startPkId	= context.getStartPk();
	private final long					endPkId	= context.getEndPk();

	public DataSendService(ConcurrentLinkedQueue<SendTask> input) {
		this.input = input;
	}

	public void start() throws IOException {

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {

					final long start = System.currentTimeMillis();
					ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
					final Socket socket = serverSocket.accept();
					final CountDownLatch latch = new CountDownLatch(1);
					new Thread(new Worker(input, socket, latch)).start();
					new Thread(new Runnable() {
						@Override
						public void run() {
							try {
								latch.await();
								OutputStream outputStream = socket.getOutputStream();
								ByteBuffer buffer = ByteBuffer.allocate(4);
								buffer.putInt(-1);
								outputStream.write(buffer.array());
								outputStream.close();
								long end = System.currentTimeMillis();
								stat.info("send finsh, cost {} ms", end - start);
								stat.info("all cost {} ms", context.getCostTime());
							} catch (InterruptedException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}).start();
				} catch (Exception ex) {

				}
			}
		}).start();

	}

	class Worker implements Runnable {
		private ConcurrentLinkedQueue<SendTask>	input;
		private Socket						socket;
		private OutputStream				outputStream;
		private ByteBuffer					buffer	= ByteBuffer.allocate(10 << 20);
		private CountDownLatch				latch;

		public Worker(ConcurrentLinkedQueue<SendTask> input, Socket socket,
				CountDownLatch latch) {
			this.input = input;
			this.socket = socket;
			this.latch = latch;
			try {
				outputStream = socket.getOutputStream();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private AtomicInteger counter = new AtomicInteger(0);

		private boolean inRange(long pk) {
			return pk > startPkId && pk < endPkId;
		}

		private void sendAns(RecordMap replayMap, int partitionId) throws IOException {
			buffer.clear();
			buffer.putInt(0);
			ArrayRecordMap arrayMap = (ArrayRecordMap) replayMap;
			buffer.putInt(partitionId);
			int[] buckets = arrayMap.getBuckets();
			int start = arrayMap.getStart();
			int end = arrayMap.getEnd();
			for (int i = start; i <= end; ++i) {
				if (buckets[i - start] != -1) {
					buffer.putLong(i);
					arrayMap.getValue(buckets[i - start], buffer);
				}
			}
			buffer.flip();
			buffer.putInt(0, buffer.limit() - 4);
			outputStream.write(buffer.array(), 0, buffer.limit());
		}

		@Override
		public void run() {
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						Timer.sleep(10000, 0);
						stat.info("counter {}", counter.get());
					}
				}
			});
			t.setDaemon(true);
			t.start();
			while (true) {
				SendTask task = input.poll();
				if (task == null) {
					continue;
				}
				if (task.isEnd()) {
					input.offer(task);
					break;
				}
				try {
					sendAns(task.getReplayMap(), task.getPartitionId());
					counter.incrementAndGet();
				} catch (Exception e) {
					//                    stat.info("exception: {}", e.getMessage());
					e.printStackTrace();
				}
			}
			latch.countDown();

		}
	}
}
