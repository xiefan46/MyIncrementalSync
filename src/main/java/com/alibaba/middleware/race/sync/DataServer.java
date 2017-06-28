/*
 * Copyright 2015-2017 GenerallyCloud.com
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.util.Map;

import com.alibaba.middleware.race.sync.channel.ByteArrayBuffer;
import com.alibaba.middleware.race.sync.channel.SingleBufferedOutputStream;
import com.alibaba.middleware.race.sync.compress.Lz4CompressedOutputStream;
import com.alibaba.middleware.race.sync.io.FixedLengthProtocolFactory;
import com.alibaba.middleware.race.sync.io.FixedLengthReadFuture;
import com.alibaba.middleware.race.sync.io.FixedLengthReadFutureImpl;
import com.alibaba.middleware.race.sync.util.RecordUtil;
import com.generallycloud.baseio.acceptor.SocketChannelAcceptor;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;
import com.generallycloud.baseio.common.MathUtil;
import com.generallycloud.baseio.component.IoEventHandleAdaptor;
import com.generallycloud.baseio.component.LoggerSocketSEListener;
import com.generallycloud.baseio.component.NioSocketChannelContext;
import com.generallycloud.baseio.component.SocketChannelContext;
import com.generallycloud.baseio.component.SocketSession;
import com.generallycloud.baseio.configuration.ServerConfiguration;
import com.generallycloud.baseio.protocol.ReadFuture;

/**
 * @author wangkai
 *
 */
public class DataServer implements Runnable {

	private static DataServer dataServer = new DataServer();

	public static DataServer get() {
		return dataServer;
	}

	private int				port;

	private SocketChannelContext	socketChannelContext;

	@Override
	public void run() {

		try {

			IoEventHandleAdaptor eventHandleAdaptor = new IoEventHandleAdaptor() {

				@Override
				public void accept(SocketSession session, ReadFuture future) throws Exception {
					//					logger.info(future.getReadText());
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

			socketChannelContext = context;
		} catch (IOException e) {
			//			logger.error(e.getMessage(),e);
		}
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void sendResultToClient(Context context) throws Exception {

		if (Constants.ENABLE_COMPRESS) {

			ByteArrayBuffer target = new ByteArrayBuffer(1024 * 1024 * 8, 4);

			int cBuffer = (int) (1024 * 512 * 1.005);

			Lz4CompressedOutputStream outputStream = new Lz4CompressedOutputStream(target,
					cBuffer);

			SingleBufferedOutputStream buffer = new SingleBufferedOutputStream(outputStream,
					1024 * 512);

			RecordUtil.writeToByteArrayBuffer(context, buffer);

			buffer.flush();

			writeToClient(target);
		} else {

			ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(Constants.RESULT_LENGTH + 4,
					4);

			RecordUtil.writeToByteArrayBuffer(context, byteArrayBuffer);

			writeToClient(byteArrayBuffer);
		}
	}

	private void writeToClient(ByteArrayBuffer buffer) {
		writeToClient(buffer.array(), buffer.size());
	}

	private void writeToClient(byte[] array, int len) {

		SocketChannelContext channelContext = this.socketChannelContext;

		Map<Integer, SocketSession> sessions = channelContext.getSessionManager()
				.getManagedSessions();

		if (sessions.size() == 0) {
			throw new RuntimeException("null client");
		}

		SocketSession session = sessions.values().iterator().next();

		FixedLengthReadFuture future = new FixedLengthReadFutureImpl(channelContext);

		MathUtil.int2Byte(array, len - 4, 0);

		future.setBuf(UnpooledByteBufAllocator.getHeapInstance().wrap(array, 0, len));

		//		logger.info("开始向客户端传送文件，当前时间：{}", System.currentTimeMillis());

		session.flush(future);
	}

}
