package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.codec.RecordLogCodec;
import com.alibaba.middleware.race.sync.model.Record;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ChannelReader {

	private static ChannelReader	channelReader	= new ChannelReader();

	//	private final int			HEAD_SKIP		= "000001:106|1489133349000|".length();

	private final int			HEAD_SKIP		= "|4e3660bf-06f5-4dd7-bebf-a17131997d0e|1422652823552|"
			.length();

	private final int			SCHEMA_SKIP	= HEAD_SKIP + 1;

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
		buf.position(end + 1);
		if (!compare(readBuffer, offset + HEAD_SKIP, tableSchema)) {
			return null;
		}
		return codec.decode(readBuffer, offset + SCHEMA_SKIP + tableSchema.length, end - 1,
				startId, endId);
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
