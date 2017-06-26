package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.codec.RecordLogCodec2;
import com.alibaba.middleware.race.sync.model.RecordLog;
import com.alibaba.middleware.race.sync.model.Table;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ByteBufReader {

	private RecordLogCodec2 codec = new RecordLogCodec2();

	public boolean read(Table table, ByteBuf buf, byte[] tableSchema, RecordLog r,int startId,int endId)
			throws IOException {
		byte[] readBuffer = buf.array();
		int offset = buf.position();
		if (!buf.hasRemaining()) {
			return false;
		}
		int off = codec.decode(table, readBuffer, tableSchema, offset, r,startId,endId);
		buf.position(off + 1);
		return r.isRead();
	}

}
