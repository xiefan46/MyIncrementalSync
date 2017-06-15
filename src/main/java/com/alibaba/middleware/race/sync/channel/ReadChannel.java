package com.alibaba.middleware.race.sync.channel;

import java.io.IOException;
import java.io.InputStream;

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 *
 */
//FIXME 文件分割时处理ReadChannel的head和tail
public abstract class ReadChannel extends InputStream {

	private ByteBuf buf;

	public ReadChannel(int maxBufferLen) {
		this.buf = UnpooledByteBufAllocator.getHeapInstance().allocate(maxBufferLen);
		this.buf.limit(0);
	}

	public ByteBuf getByteBuf(){
		return buf;
	}

	public abstract boolean hasRemaining();
	
	public boolean hasBufRemaining(){
		return hasRemaining() || buf.hasRemaining();
	}

	public abstract int read(ByteBuf buf) throws IOException;

}
