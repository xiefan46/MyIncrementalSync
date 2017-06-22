package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * @author wangkai
 */
public class ByteBufReader {

	private static final Logger	logger		= LoggerFactory.getLogger(ByteBufReader.class);

	private static ByteBufReader	channelReader	= new ByteBufReader();

	public static ByteBufReader get() {
		return channelReader;
	}

	private ByteBufReader() {
	}

	private RecordLogCodec2	codec	= RecordLogCodec2.get();

	private AtomicInteger	readCount	= new AtomicInteger(0);

	public boolean read(Table table, ByteBuf buf, byte[] tableSchema, RecordLog r)
			throws IOException {
		byte[] readBuffer = buf.array();
		int offset = buf.position();
		if (!buf.hasRemaining()) {
			return false;
		}
		int off = codec.decode(table, readBuffer, tableSchema, offset, r);
		buf.position(off + 1);
		readCount.incrementAndGet();
		if (readCount.get() % 100000 == 0) {
			logger.info("扫描条目数 : {}", readCount.get());
		}
		return true;
	}

}
