package com.alibaba.middleware.race.sync.other.bytes;

import java.io.IOException;
import java.io.InputStream;

import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 *
 */
public class ByteBufUtil {

	public static int read(ByteBuf buf,InputStream inputStream) throws IOException{
		byte [] array = buf.array();
		if (!buf.hasRemaining()) {
			int len = inputStream.read(array,0,buf.capacity());
			if (len > 0) {
				buf.position(0);
				buf.limit(len);
			}
			return len;
		}
		int remaining = buf.remaining();
		System.arraycopy(array, buf.position(), array, 0, remaining);
		buf.position(0);
		buf.limit(remaining);
		int len = inputStream.read(array,remaining,buf.capacity() - remaining);
		if (len > 0) {
			buf.limit(remaining + len);
		}
		return len;
	}
	
}
