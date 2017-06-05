package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.io.FixedLengthProtocolFactory;
import com.alibaba.middleware.race.sync.io.FixedLengthReadFuture;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.common.CloseUtil;
import com.generallycloud.baseio.component.IoEventHandleAdaptor;
import com.generallycloud.baseio.component.LoggerSocketSEListener;
import com.generallycloud.baseio.component.NioSocketChannelContext;
import com.generallycloud.baseio.component.SocketChannelContext;
import com.generallycloud.baseio.component.SocketSession;
import com.generallycloud.baseio.configuration.ServerConfiguration;
import com.generallycloud.baseio.connector.SocketChannelConnector;
import com.generallycloud.baseio.protocol.ReadFuture;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {

	private Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		initProperties();

		Client client = new Client();
		client.connect(args[0], Constants.SERVER_PORT);
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
	@SuppressWarnings("resource")
	public void connect(String host, int port) throws Exception {

		logger.info("Welcome");

		IoEventHandleAdaptor eventHandleAdaptor = new IoEventHandleAdaptor() {
			@Override
			public void accept(SocketSession session, ReadFuture future) throws Exception {
				FixedLengthReadFuture f = (FixedLengthReadFuture) future;
				ByteBuf buf = f.getBuf();
				writeToFile(buf);
				CloseUtil.close(session);
			}
		};

		ServerConfiguration configuration = new ServerConfiguration(host, port);

		configuration.setSERVER_ENABLE_MEMORY_POOL(false);

		SocketChannelContext context = new NioSocketChannelContext(configuration);

		SocketChannelConnector connector = new SocketChannelConnector(context);

		context.setIoEventHandleAdaptor(eventHandleAdaptor);

		context.addSessionEventListener(new LoggerSocketSEListener());

		context.setProtocolFactory(new FixedLengthProtocolFactory());

		connector.connect();
	}

	private void writeToFile(ByteBuf buf) {
		OutputStream outputStream = null;
		try {
			String fileName = Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME;
			RandomAccessFile raf;
			raf = new RandomAccessFile(new File(fileName), "rw");
			outputStream = new RAFOutputStream(raf);
			outputStream.write(buf.array(), 4, buf.limit());
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}finally{
			CloseUtil.close(outputStream);
		}
	}

}
