package com.alibaba.middleware.race.sync.service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Config;
import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.common.BufferPool;
import com.alibaba.middleware.race.sync.entity.ParseTask;
import com.alibaba.middleware.race.sync.util.Timer;

/**
 * Created by xiefan on 6/24/17.
 */
public class DataLoadService {

    private static Logger stat = LoggerFactory.getLogger("stat");
    private ConcurrentLinkedQueue<ParseTask> output;

    private Object waitForLoader = new Object();

    public DataLoadService(ConcurrentLinkedQueue<ParseTask> output) {
        this.output = output;
    }

    public void start() {
        final long start = System.currentTimeMillis();
        new Thread(new DataLoader()).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (waitForLoader) {
                    try {
                        waitForLoader.wait();
                        long end = System.currentTimeMillis();
                        stat.info("Load finish, cost {} ms", end - start);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }


    public void finish() {

    }

    class DataLoader implements Runnable {

        private Context context = Context.getInstance();
        private BufferPool bufferPool = context.getReadBufferPool();
        private ByteBuffer remainBuffer = ByteBuffer.allocate(Config.READ_BUFFER_SIZE);

        @Override
        public void run() {
            byte[] tmp = new byte[1024];
            remainBuffer.clear();
            long epoch = 0;
            long totalSize = 0;
            for (int i = 1; i <= 10; ++i) {
                String filename = String.format("%s/%d.txt", Constants.DATA_HOME, i);
                try {
                    RandomAccessFile raf = new RandomAccessFile(filename, "r");
                    InputStream inputStream = new FileInputStream(filename);
                    FileChannel fc = raf.getChannel();
                    long fileSize = fc.size();
                    long size = 0;
                    while (true) {
                        ByteBuffer buffer = null;
                        while ((buffer = bufferPool.getBuffer()) == null) {
                            Timer.sleep(1, 0);
                            continue;
                        }
                        remainBuffer.flip();
                        buffer.put(remainBuffer);
                        remainBuffer.clear();

                        int len = fc.read(buffer);
                        buffer.flip();

                        if (len <= 0) {
//                            System.out.println("fuck");
                            if (buffer.position() != 0) {
                                buffer.flip();
                                output.offer(new ParseTask(buffer, epoch++));
                            }
                            break;
                        }

                        size += len;
                        for (int j = buffer.limit() - 1; j >= 0; --j) {
                            if (buffer.get(j) == '\n') {
                                buffer.position(j+1);
                                remainBuffer.put(buffer);
                                buffer.limit(j+1);
                                buffer.position(0);
                                break;
                            }
                        }
                        output.offer(new ParseTask(buffer, epoch++));
                    }
                    inputStream.close();
                    stat.info("{}: {}", filename, size);
                    totalSize += size;
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            stat.info("load total load size: " + totalSize);
            output.offer(ParseTask.END_TASK);
            synchronized (waitForLoader) {
                waitForLoader.notify();
            }
        }
    }
}


