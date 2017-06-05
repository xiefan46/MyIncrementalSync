package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.codec.RecordLogCodec;
import com.alibaba.middleware.race.sync.model.Record;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ChannelReader {

	private static ChannelReader channelReader = new ChannelReader();

	public static ChannelReader get() {
		return channelReader;
	}

	private RecordLogCodec codec = RecordLogCodec.get();

	public Record read(ReadChannel channel, byte[] tableSchema, long startId, long endId)
			throws IOException {
		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int limit = buf.limit();
		int offset = buf.position();
		if (buf.remaining() == 0) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema, startId, endId);
		}

		int end = findNextChar(readBuffer, offset, limit, '\n');
		if (end == -1) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema, startId, endId);
		}
		buf.position(offset + end + 1);
		if (!compare(readBuffer, offset + 25, tableSchema)) {
			return null;
		}
		return codec.decode(readBuffer, offset + 26 + tableSchema.length, end - 1, startId,
				endId);
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
		System.out.println();
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
