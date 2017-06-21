package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * @author wangkai
 */
public class ChannelReader2 {

	private static ChannelReader2	channelReader	= new ChannelReader2();

	private final int			MAX_RECORD_LEN	= 1024;

	private static final Logger	logger		= LoggerFactory.getLogger(ChannelReader2.class);

	public static ChannelReader2 get() {
		return channelReader;
	}

	private ChannelReader2() {
	}

	private RecordLogCodec2 codec = RecordLogCodec2.get();

	public RecordLog read(ReadChannel channel, byte[] tableSchema, RecordLog r)
			throws IOException {
		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int offset = buf.position();
		if (buf.remaining() < MAX_RECORD_LEN) {
			if (!channel.hasRemaining()) {
				if (buf.remaining() > 1) {
					int off = codec.decode(readBuffer, tableSchema, offset, r);
					buf.position(off + 1);
					/*
					 * if (print(r)) logger.info("record : {}", new
					 * String(readBuffer, offset, off - offset));
					 */
					return r;
				}
				return null;
			}
			channel.read(buf);

			return read(channel, tableSchema, r);
		}
		int off = codec.decode(readBuffer, tableSchema, offset, r);
		/*
		 * if (print(r)) logger.info("record : {}", new String(readBuffer,
		 * offset, off - offset));
		 */
		buf.position(off + 1);
		return r;
	}

	private boolean print(RecordLog recordLog) {
		if (recordLog.getPrimaryColumn().getBeforeValue() == 601
				|| recordLog.getPrimaryColumn().getLongValue() == 601)
			return true;
		return false;
	}

}
