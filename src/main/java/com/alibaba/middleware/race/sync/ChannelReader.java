package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.codec.RecordLogCodec;
import com.alibaba.middleware.race.sync.model.Record;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 *
 */
public class ChannelReader {

	private static ChannelReader channelReader = new ChannelReader();

	public static ChannelReader get() {
		return channelReader;
	}

	private RecordLogCodec codec = RecordLogCodec.get();

	protected Record read(ReadChannel channel, byte[] tableSchema) throws IOException {
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
		int end = findNextChar(readBuffer, offset, limit, '\n');
		if (end == -1) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema);
		}
		buf.position(offset + end);
		if (!compare(readBuffer, offset, tableSchema)) {
			return null;
		}
		return codec.decode(readBuffer, offset + 26 + tableSchema.length, end);
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
		for (; offset < end;) {
			if (data[++offset] == c) {
				return offset;
			}
		}
		return -1;
	}

}