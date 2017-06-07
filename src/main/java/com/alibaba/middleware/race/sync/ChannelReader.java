package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.codec.RecordLogCodec;
import com.alibaba.middleware.race.sync.model.Record;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * @author wangkai
 */
public class ChannelReader {

	private static ChannelReader	channelReader	= new ChannelReader();

	private final int			HEAD_SKIP		= "|mysql-bin.00001717148759|1496736165000"
			.length();

	private static final Logger	logger		= LoggerFactory.getLogger(Server.class);

	public static ChannelReader get() {
		return channelReader;
	}

	private ChannelReader() {
	}

	private RecordLogCodec codec = RecordLogCodec.get();

	public Record read(ReadChannel channel, byte[] tableSchema) throws IOException {

		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int limit = buf.limit();
		int offset = buf.position();
		try {
			if (limit - offset < HEAD_SKIP) {
				if (!channel.hasRemaining()) {
					return null;
				}
				channel.read(buf);
				return read(channel, tableSchema);
			}
			int skip = offset + HEAD_SKIP;
			int end = findNextChar(readBuffer, skip, limit, '\n');
			if (end == -1) {
				if (!channel.hasRemaining()) {
					return null;
				}
				channel.read(buf);
				return read(channel, tableSchema);
			}
			buf.position(end + 1);
			offset = findNextChar(readBuffer, skip, end, '|');
			if (!compare(readBuffer, ++offset, tableSchema)) {
				return null;
			}
			return codec.decode(readBuffer, offset + 1 + tableSchema.length, end - 1);
		} catch (Exception e) {
			if (Constants.COLLECT_STAT) {
				logger.error(e.getMessage());
				return null;
			} else {
				throw e;
			}
		}

	}

	private boolean compare(byte[] data, int offset, byte[] tableSchema) {
		for (int i = 0; i < tableSchema.length; i++) {
			if (tableSchema[i] != data[offset + i]) {
				return false;
			}
		}
		return true;
	}

	private int findNextChar(byte[] data, int offset, int end, char c) {
		if (offset >= end) {
			return -1;
		}
		for (;;) {
			if (data[offset] == c) {
				return offset;
			}
			if (++offset == end) {
				return -1;
			}
		}
	}

}
