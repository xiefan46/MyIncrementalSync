package com.alibaba.middleware.race.sync;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.io.FixedLengthProtocolFactory;
import com.generallycloud.baseio.acceptor.SocketChannelAcceptor;
import com.generallycloud.baseio.component.IoEventHandleAdaptor;
import com.generallycloud.baseio.component.LoggerSocketSEListener;
import com.generallycloud.baseio.component.NioSocketChannelContext;
import com.generallycloud.baseio.component.SocketChannelContext;
import com.generallycloud.baseio.component.SocketSession;
import com.generallycloud.baseio.configuration.ServerConfiguration;
import com.generallycloud.baseio.protocol.ReadFuture;

/**
 * 服务器类，负责push消息到client Created by wanshao on 2017/5/25.
 */
public class Server {
	
	private static Server server = new Server();
	
	public static Server get(){
		return server;
	}
	
	private SocketChannelContext socketChannelContext;
	
	private static Logger logger = LoggerFactory.getLogger(Server.class);
	
	private MainThread mainThread;

	public static void main(String[] args) throws Exception {
		initProperties();

		Server server = get();

		server.startServer1(args,5527);

		logger.info("com.alibaba.middleware.race.sync.Server is running....");
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
	 * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware
	 * student 100 200
	 * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间
	 * 对应DB的SQL为： select * from middleware.student where id>100 and id<200
	 */
	private void startServer1(String[] args,int port) throws IOException {
		// 第一个参数是Schema Name
		logger.info("tableSchema:" + args[0]);
		// 第二个参数是Schema Name
		logger.info("table:" + args[1]);
		// 第三个参数是start pk Id
		logger.info("start:" + args[2]);
		// 第四个参数是end pk Id
		logger.info("end:" + args[3]);
		
		String schema = args[0];
		String table = args[1];
		long startId = Long.parseLong(args[2]);
		long endId = Long.parseLong(args[3]);

		IoEventHandleAdaptor eventHandleAdaptor = new IoEventHandleAdaptor() {

			@Override
			public void accept(SocketSession session, ReadFuture future) throws Exception {
				logger.info(future.getReadText());
			}
		};

		ServerConfiguration configuration = new ServerConfiguration(port);

		configuration.setSERVER_CORE_SIZE(1);
		configuration.setSERVER_ENABLE_MEMORY_POOL(false);

		SocketChannelContext context = new NioSocketChannelContext(configuration);

		SocketChannelAcceptor acceptor = new SocketChannelAcceptor(context);

		context.addSessionEventListener(new LoggerSocketSEListener());
		
		context.setIoEventHandleAdaptor(eventHandleAdaptor);

		context.setProtocolFactory(new FixedLengthProtocolFactory());

		acceptor.bind();
		
		mainThread = new MainThread(new RecordLogReceiverImpl(), schema, table, startId, endId);
		
		new Thread(mainThread).start();
	}
	
	public MainThread getMainThread() {
		return mainThread;
	}
	
	public SocketChannelContext getSocketChannelContext() {
		return socketChannelContext;
	}
	
}
