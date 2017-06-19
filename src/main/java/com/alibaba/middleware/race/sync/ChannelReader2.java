package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.model.RecordLog;
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

	public boolean read(ReadChannel channel, byte[] tableSchema, RecordLog r) throws IOException {
		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int offset = buf.position();
		if (buf.remaining() < MAX_RECORD_LEN) {
			if (!channel.hasRemaining()) {
				if (buf.remaining() > 1) {
					int off = codec.decode(readBuffer, tableSchema, offset, r);
					buf.position(off + 1);
					return true;
				}
				return false;
			}
			channel.read(buf);
			return read(channel, tableSchema, r);
		}
		int off = codec.decode(readBuffer, tableSchema, offset, r);
		buf.position(off + 1);
		return true;
	}

	public static boolean print(RecordLog r) {
		if (r.getPrimaryColumn().getLongValue() == 606
				|| r.getPrimaryColumn().getBeforeValue() == 606
				|| r.getPrimaryColumn().getLongValue() == 1000606
				|| r.getPrimaryColumn().getBeforeValue() == 1000606) {
			return true;
		}
		return false;
	}

}
