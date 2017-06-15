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

	private final int			HEAD_SKIP		= "|mysql-bin.00001717148759|1496736165000".length();

	public static ChannelReader get() {
		return channelReader;
	}

	private ChannelReader() {
	}
	
	private int maxRecordLen;

	private RecordLogCodec codec = RecordLogCodec.get();

	public RecordLog read(ReadChannel channel, byte[] tableSchema, RecordLog r) throws IOException {
		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int limit = buf.limit();
		int offset = buf.position();
		if (buf.remaining() == 0) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema, r);
		}
		if (limit - offset < HEAD_SKIP) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema, r);
		}
		int end = findNextChar(readBuffer, offset + HEAD_SKIP, limit, '\n');
		int len = end - offset;
		if (len > maxRecordLen) {
			maxRecordLen = len;
		}
		if (end == -1) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema, r);
		}
		buf.position(end + 1);
		return codec.decode(readBuffer, tableSchema, offset, end - 1, r);
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
	
	public int getMaxRecordLen() {
		return maxRecordLen;
	}

}
