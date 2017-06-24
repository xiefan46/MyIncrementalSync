package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.util.Map;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ChannelReader2 {

	private static ChannelReader2	channelReader	= new ChannelReader2();

	private final int			MAX_RECORD_LEN	= 1024;

	public static ChannelReader2 get() {
		return channelReader;
	}

	private ChannelReader2() {
	}

	private RecordLogCodec2 codec = RecordLogCodec2.get();

	public boolean read(RecalculateContext context, ReadChannel channel, byte[] tableSchema)
			throws IOException {
		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int offset = buf.position();
		if (buf.remaining() < MAX_RECORD_LEN) {
			if (!channel.hasRemaining()) {
				if (buf.remaining() > 1) {
					int off = codec.decode(context, readBuffer, tableSchema, offset);
					buf.position(off + 1);
					return true;
				}
				return false;
			}
			channel.read(buf);
			return read(context, channel, tableSchema);
		}
		int off = codec.decode(context, readBuffer, tableSchema, offset);
		buf.position(off + 1);
		return true;
	}

}
