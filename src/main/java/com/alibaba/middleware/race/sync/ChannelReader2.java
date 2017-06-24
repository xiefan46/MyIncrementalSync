package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
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

	public boolean read(Context context, ReadChannel channel, byte[] tableSchema)
			throws IOException {
		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int offset = buf.position();
		if (buf.remaining() < MAX_RECORD_LEN) {
			if (!channel.hasRemaining()) {
				if (buf.remaining() > 1) {
					int off = codec.decode(context, readBuffer, tableSchema, offset);
					buf.position(off + 1);
					/*if (codec.print) {
						logger.info("record : {}",
								new String(readBuffer, offset, off - offset));
					}*/
					return true;
				}
				return false;
			}
			channel.read(buf);
			return read(context, channel, tableSchema);
		}
		int off = codec.decode(context, readBuffer, tableSchema, offset);
		/*if (codec.print) {
			logger.info("record : {}", new String(readBuffer, offset, off - offset));
		}*/
		buf.position(off + 1);
		return true;
	}

}
