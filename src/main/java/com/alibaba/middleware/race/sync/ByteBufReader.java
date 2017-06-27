package com.alibaba.middleware.race.sync;

import java.io.IOException;

import com.alibaba.middleware.race.sync.codec.RecordLogCodec3;
import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 */
public class ByteBufReader {

	private RecordLogCodec3 codec = new RecordLogCodec3();

	public void read(Context context, ReadTask task,byte[] tableSchema)
			throws IOException {
		ByteBuf buf = task.getBuf();
		int version = task.getVersion();
		byte[] readBuffer = buf.array();
		int off = 0;
		int limit = buf.limit();
		for(;off < limit;){
			off = codec.decode(context,version, readBuffer, tableSchema, off) + 1;
		}
	}

}
