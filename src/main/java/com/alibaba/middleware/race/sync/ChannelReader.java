package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ChannelReader {

	private static ChannelReader	channelReader	= new ChannelReader();

	private final int			HEAD_SKIP		= "|mysql-bin.00001717148759|1496736165000"
			.length();

	public static ChannelReader get() {
		return channelReader;
	}

	private ChannelReader() {
	}

	private RecordLogCodec codec = RecordLogCodec.get();

	public RecordLog read(ReadChannel channel, byte[] tableSchema) throws IOException {
		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int limit = buf.limit();
		int offset = buf.position();
		if (buf.remaining() == 0) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema);
		}
		if (limit - offset < HEAD_SKIP) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema);
		}
		int end = findNextChar(readBuffer, offset + HEAD_SKIP, limit, '\n');
		if (end == -1) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema);
		}
		buf.position(end + 1);
		return codec.decode(readBuffer, tableSchema, offset, end - 1);
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
