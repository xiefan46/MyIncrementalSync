package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.channel.RAFOutputStream;
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

		if (args == null || args.length == 0) {
			args = new String[] { "127.0.0.1" };
		}

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

		logger.info("Welcome,{}", System.currentTimeMillis());

		IoEventHandleAdaptor eventHandleAdaptor = new IoEventHandleAdaptor() {
			@Override
			public void accept(SocketSession session, ReadFuture future) throws Exception {
				FixedLengthReadFuture f = (FixedLengthReadFuture) future;
				ByteBuf buf = f.getBuf();
				logger.info("客户端收到文件。当前时间：{}. 文件大小 : {} B", System.currentTimeMillis(),
						buf.limit());
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
		String fileName = Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME;
		try {
			printResult(buf);
			long startTime = System.currentTimeMillis();
			File f = new File(fileName);
			if (f.exists()) {
				f.delete();
			}
			f.createNewFile();
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			outputStream = new RAFOutputStream(raf);
			outputStream.write(buf.array(), 0, buf.limit());
			outputStream.flush();
			logger.info("写结果文件到本地文件系统耗时 : {}", System.currentTimeMillis() - startTime);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			CloseUtil.close(outputStream);
		}
		//generate md5
		try {
			logger.info("Result file md5 : " + generateMD5(fileName));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void printResult(ByteBuf buf) {
		String str = new String(buf.array(), 0, buf.limit());
		logger.info("r:");
		logger.info(str);
	}

	private String generateMD5(String path) throws Exception {
		long startTime = System.currentTimeMillis();
		String strMD5 = null;
		File file = new File(path);
		FileInputStream in = new FileInputStream(file);
		MappedByteBuffer buffer = in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0,
				file.length());
		MessageDigest digest = MessageDigest.getInstance("md5");
		digest.update(buffer);
		in.close();

		byte[] byteArr = digest.digest();
		BigInteger bigInteger = new BigInteger(1, byteArr);
		strMD5 = bigInteger.toString(16);
		logger.info("Generate md5 cost time {}", System.currentTimeMillis() - startTime);
		return strMD5;
	}

}
