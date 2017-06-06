package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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

	//	private final int			HEAD_SKIP		= "000001:106|1489133349000|".length();

	/*
	 * private final int HEAD_SKIP =
	 * "|mysql-bin.000017595228308|1496737831000|" .length();
	 */

	//												|mysql-bin.00001717165437|1496736165000|

	//												|mysql-bin.000017659728407|1496746079000|

	//private final int			SCHEMA_SKIP	= HEAD_SKIP + 1;

	private final int			TS_SKIP		= "1489133349000|".length();

	public static ChannelReader get() {
		return channelReader;
	}

	private static final Logger logger = LoggerFactory.getLogger(ChannelReader.class);

	private ChannelReader() {
	}

	private RecordLogCodec codec = RecordLogCodec.get();

	// |a|
	public Record read(ReadChannel channel, byte[] tableSchema, long startId, long endId)
			throws IOException {

		ByteBuf buf = channel.getByteBuf();
		byte[] readBuffer = buf.array();
		int offset = 0;
		int end = 0;
		int limit = buf.limit();
		offset = buf.position();
		if (buf.remaining() == 0) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema, startId, endId);
		}
		int flag = findNextChar(readBuffer, offset + 1, limit, '|');
		int idSkip = flag - offset + 1;
		int headSkip = idSkip + TS_SKIP;
		int schemaSkip = headSkip + 1;
		end = findNextChar(readBuffer, offset, limit, '\n');
		if (end == -1) {
			if (!channel.hasRemaining()) {
				return null;
			}
			channel.read(buf);
			return read(channel, tableSchema, startId, endId);
		}
		buf.position(end + 1);
		if (!compare(readBuffer, offset + headSkip, tableSchema)) {
			logger.debug("filter : table schema. My tableschema : " + new String(tableSchema));
			return null;
		}
		return codec.decode(readBuffer, offset + schemaSkip + tableSchema.length, end - 1,
				startId, endId);

	}

	private boolean compare(byte[] data, int offset, byte[] tableSchema) {

		String str = null;
		try {
			str = new String(data, offset, tableSchema.length, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		//logger.debug("Actural table schema : " + str);

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
