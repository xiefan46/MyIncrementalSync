package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.alibaba.middleware.race.sync.channel.ReadChannel;
import com.alibaba.middleware.race.sync.codec.RecordLogCodec;
import com.alibaba.middleware.race.sync.model.RecordLog;
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

	private static final Logger	logger		= LoggerFactory.getLogger(ChannelReader.class);

	public static ChannelReader get() {
		return channelReader;
	}

	private ChannelReader() {
	}

	private RecordLogCodec	codec	= RecordLogCodec.get();

	int					count2	= 0;

	private List<Long>		logIdList	= Arrays.asList(621L, 170001L, 1089L);

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
		RecordLog r = codec.decode(readBuffer, tableSchema, offset, end - 1);
		if (r != null) {
			if (count2 < 50 && (logIdList.contains(r.getPrimaryColumn().getLongValue())
					|| logIdList.contains(r.getPrimaryColumn().getBeforeValue()))) {
				String str = new String(readBuffer, offset, end - offset);
				logger.info("Record log : " + str);
				count2++;
			}
		}
		return r;
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
