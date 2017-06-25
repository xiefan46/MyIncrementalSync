package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.sync.service.ReaderThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.model.Block;
import com.alibaba.middleware.race.sync.model.SendTask;
import com.alibaba.middleware.race.sync.service.Reader;
import com.alibaba.middleware.race.sync.service.DataParseService;
import com.alibaba.middleware.race.sync.service.DataReplayService;
import com.alibaba.middleware.race.sync.service.DataSendService;

public class Server {

	// 保存channel
	// 接收评测程序的三个参数

	private String			schema;
	private String			table;
	private int			startPkId;
	private int			endPkId;

	private static Logger	logger	= LoggerFactory.getLogger(Server.class);

	private static Context	context	= Context.getInstance();

	private void getArgs(String[] args) {
		schema = args[0];
		table = args[1];
		startPkId = Integer.valueOf(args[2]);
		endPkId = Integer.valueOf(args[3]);
	}

	public Server(String[] args) {
		getArgs(args);
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		logger.info("start " + System.currentTimeMillis());
		initProperties();
		printInput(args);
		initContext(args);

		Server server = new Server(args);
		server.startServer(5527);
	}

	private static void initContext(String[] args) throws IOException {
		context.initQuery(args[0], args[1], Long.valueOf(args[2]), Long.valueOf(args[3]));
	}

	/**
	 * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware
	 * student 100 200
	 * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间
	 * 对应DB的SQL为： select * from middleware.student where id>100 and id<200
	 */
	private static void printInput(String[] args) {
		StringBuilder sb = new StringBuilder();
		sb.append("Schema: ").append(args[0]).append("\n").append("Table: ").append(args[1])
				.append("\n").append("start: ").append(args[2]).append("\n").append("end: ")
				.append(args[3]);
		logger.info("\n{}", sb.toString());
	}

	/**
	 * 初始化系统属性
	 */
	private static void initProperties() {
		System.setProperty("middleware.test.home", Constants.TESTER_HOME);
		System.setProperty("middleware.teamcode", Constants.TEAMCODE);
		System.setProperty("app.logging.level", Constants.LOG_LEVEL);
	}

	private void startServer(int port) throws InterruptedException, IOException {

		ConcurrentLinkedQueue<SendTask> sendTaskQueue = new ConcurrentLinkedQueue<>();
		DataSendService sendService = new DataSendService(sendTaskQueue);
		sendService.start();

		ConcurrentLinkedQueue<Block> parseTaskQueue = new ConcurrentLinkedQueue<>();
		ReaderThread reader = new ReaderThread(Context.getInstance(), parseTaskQueue);
		Thread readThread = new Thread(reader);
		readThread.start();

		DataReplayService inRangeReplayService = new DataReplayService(sendTaskQueue, true,
				Config.INRANGE_REPLAYER_COUNT);
		DataReplayService outRangeReplayService = new DataReplayService(sendTaskQueue, false,
				Config.OUTRANGE_REPLAYER_COUNT);
		inRangeReplayService.start();
		outRangeReplayService.start();

		DataParseService parseService = new DataParseService(parseTaskQueue, inRangeReplayService,
				outRangeReplayService);
		parseService.start();

		readThread.join();
	}
}
