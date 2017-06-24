package com.alibaba.middleware.race.sync;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.database.Column;
import com.alibaba.middleware.race.sync.entity.ParseTask;
import com.alibaba.middleware.race.sync.entity.WriteTask;
import com.alibaba.middleware.race.sync.util.Timer;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {

	private static Logger	logger	= LoggerFactory.getLogger(Client.class);

	private final static int	port		= Constants.SERVER_PORT;
	// idle时间
	private static String	ip;
	private Socket			socket;
	private long			startTime	= System.currentTimeMillis();

	public static void main(String[] args) throws Exception {
		//        Thread.sleep(240000);
		initProperties();
		Logger logger = LoggerFactory.getLogger(Client.class);
		logger.info("Welcome: " + System.currentTimeMillis());
		// 从args获取server端的ip
		if (args.length < 1) {
			ip = "127.0.0.1";
		} else {
			ip = args[0];
		}
		Client client = new Client();
		client.connect(ip, port);
		client.start();
		client.newAnswer();

	}

	/**
	 * 初始化系统属性
	 */
	private static void initProperties() {
		System.setProperty("middleware.test.home", Constants.TESTER_HOME);
		System.setProperty("middleware.teamcode", Constants.TEAMCODE);
		System.setProperty("app.logging.level", Constants.LOG_LEVEL);
	}

	/**
	 * 连接服务端
	 *
	 * @param host
	 * @param port
	 * @throws Exception
	 */
	public void connect(String host, int port) {
		while (true) {
			try {
				socket = new Socket(host, port);
			} catch (IOException e) {
				Timer.sleep(1, 0);
				continue;
			}
			try {
				socket.setKeepAlive(true);
				break;
			} catch (SocketException e) {
				e.printStackTrace();
			}
		}
		logger.info("connected");
	}

	public void start() throws IOException {
		int count = 8;

		new Thread(new Writer()).start();

		final CountDownLatch latch = new CountDownLatch(count);
		for (int i = 0; i < count; ++i) {
			Parser parser = new Parser(latch);
			new Thread(parser).start();
		}

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					latch.await();
					writeTasks.offer(WriteTask.END_TASK);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	private ClientContext					context		= new ClientContext();
	List<Column>							columnList	= context.getColumnList();

	private ConcurrentLinkedQueue<ParseTask>	parseTasks	= new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<WriteTask>	writeTasks	= new ConcurrentLinkedQueue<>();

	private void newAnswer() throws IOException {
		ByteBuffer tmpBuffer = ByteBuffer.allocate(4);
		InputStream inputStream = socket.getInputStream();
		int valSize = 0;
		while (true) {
			int len = 0;
			inputStream.read(tmpBuffer.array());
			int totalLen = tmpBuffer.getInt(0);
			if (totalLen == -1) {
				parseTasks.offer(ParseTask.END_TASK);
				break;
			}
			byte[] data = new byte[totalLen];
			int tmp = totalLen;
			valSize += totalLen - 4;
			int pos = 0;
			while (totalLen > 0) {
				len = inputStream.read(data, pos, totalLen);
				totalLen -= len;
				pos += len;
			}
			ByteBuffer buffer = ByteBuffer.wrap(data, 0, tmp);
			parseTasks.offer(new ParseTask(buffer, 0));
		}

		int totalCount = valSize / (columnList.size() * 8);
		System.out.println(totalCount);
		System.out.println(valSize % (columnList.size() * 8));
	}

	class Parser implements Runnable {
		private CountDownLatch latch;

		public Parser(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public void run() {
			while (true) {
				ParseTask task = parseTasks.poll();
				if (task == null) {
					Timer.sleep(1, 0);
					continue;
				}
				if (task.isEnd()) {
					parseTasks.offer(task);
					break;
				}
				ByteBuffer buffer = task.getBuffer();
				StringBuilder sb = new StringBuilder();
				int partitionId = buffer.getInt();
				while (buffer.hasRemaining()) {
					sb.append(buffer.getLong());
					for (int i = 1; i < columnList.size(); ++i) {
						sb.append("\t");
						Column column = columnList.get(i);
						if (column.isInt()) {
							sb.append(buffer.getLong());
						} else {
							short len = buffer.getShort();
							sb.append(new String(buffer.array(), buffer.position(), len));
							buffer.position(buffer.position() + 6);
						}
					}
					sb.append("\n");
				}
				writeTasks.offer(new WriteTask(partitionId, sb.toString()));

			}
			latch.countDown();
		}
	}

	class Writer implements Runnable {
		private Logger				logger		= LoggerFactory.getLogger(Client.class);
		private int				now			= 0;
		private Map<Integer, String>	map			= new HashMap<>();
		private FileWriter			fileWriter	= new FileWriter(Constants.RESULT_FILE);

		private long				writeCost		= 0;

		Writer() throws IOException {
		}

		@Override
		public void run() {
			while (true) {
				WriteTask writeTask = writeTasks.poll();
				if (writeTask == null) {
					Timer.sleep(1, 0);
					continue;
				}
				if (writeTask.isEnd()) {
					break;
				}
				map.put(writeTask.getPartitionId(), writeTask.getAns());
				work();
			}
			work();
			try {
				fileWriter.close();
				logger.info("write cost {} ms", writeCost);
				logger.info("real cost {} ms", System.currentTimeMillis() - startTime);
				Timer.sleep(20000, 0);
				logger.info("display cost {} ms", System.currentTimeMillis() - startTime);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void work() {
			long start = System.currentTimeMillis();
			while (map.containsKey(now)) {
				String s = map.get(now++);
				try {
					fileWriter.write(s);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			long end = System.currentTimeMillis();
			writeCost += (end - start);
		}
	}

	private void answer() throws IOException {
		ByteBuffer tmpBuffer = ByteBuffer.allocate(4);
		byte[] buf = new byte[200 << 20];
		InputStream inputStream = socket.getInputStream();
		int pos = 0;
		int valSize = 0;
		while (true) {
			int len = 0;
			inputStream.read(tmpBuffer.array());
			int totalLen = tmpBuffer.getInt(0);
			//            System.out.println("total len: " + totalLen);
			if (totalLen == -1) {
				break;
			}
			valSize += totalLen;
			while (totalLen > 0) {
				len = inputStream.read(buf, pos, totalLen);
				totalLen -= len;
				pos += len;
			}
		}

		int totalCount = valSize / (columnList.size() * 8);
		System.out.println(totalCount);
		System.out.println(valSize % (columnList.size() * 8));

		// 排序加写答案
		ByteBuffer buffer = ByteBuffer.wrap(buf);
		long[][] tmp = new long[totalCount][2];
		int off = 0;
		for (int i = 0; i < totalCount; ++i) {
			tmp[i][0] = buffer.getLong(off);
			tmp[i][1] = off;
			off += columnList.size() * 8;
		}

		//        System.exit(1);

		logger.info("ffffffffff");

		Arrays.sort(tmp, new Comparator<long[]>() {
			@Override
			public int compare(long[] o1, long[] o2) {
				if (o1[0] > o2[0])
					return 1;
				if (o1[0] < o2[0])
					return -1;
				return 0;
			}
		});

		BufferedWriter writer = new BufferedWriter(new FileWriter(Constants.RESULT_FILE));
		for (int i = 0; i < totalCount; ++i) {
			off = (int) tmp[i][1];
			writer.write(tmp[i][0] + "");
			for (int j = 1; j < columnList.size(); ++j) {
				off += 8;
				writer.write("\t");
				Column column = columnList.get(j);
				if (column.isInt()) {
					long t = buffer.getLong(off);
					writer.write(t + "");
				} else {
					int len = buffer.getShort(off);
					String t = new String(buf, off + 2, len);
					writer.write(t);
				}
			}
			writer.write("\n");
		}
		writer.flush();
		writer.close();
		//        try {
		//            Thread.sleep(3000);
		//        } catch (InterruptedException e) {
		//            e.printStackTrace();
		//        }

	}

}
