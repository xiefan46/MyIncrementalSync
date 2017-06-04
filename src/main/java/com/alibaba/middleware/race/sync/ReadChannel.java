package com.alibaba.middleware.race.sync;

import java.io.IOException;
import java.io.InputStream;

import com.alibaba.middleware.race.sync.other.bytes.ByteBufUtil;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;
import com.generallycloud.baseio.common.CloseUtil;

/**
 * @author wangkai
 *
 */
//FIXME 文件分割时处理ReadChannel的head和tail
public class ReadChannel extends InputStream {

	private ByteBuf	buf;

	private String		fileName;

	private InputStream	inputStream;
	
	private String		tqName;
	
	private String 	tqPrefix;

	private boolean	hasRemaining	= true;

	public ReadChannel(String fileName, InputStream inputStream, int maxBufferLen) {
		String [] ss = fileName.split("\\.");
		this.tqName = ss[0];
		this.tqPrefix = ss[1];
		this.fileName = fileName;
		this.inputStream = inputStream;
		this.buf = UnpooledByteBufAllocator.getHeapInstance().allocate(maxBufferLen);
	}

	public int read(ByteBuf buf) throws IOException {
		int len = ByteBufUtil.read(buf, inputStream);
		if (len == -1) {
			hasRemaining = false;
		}
		return len;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public boolean hasRemaining() {
		return hasRemaining;
	}

	@Override
	public int read() throws IOException {
		return inputStream.read();
	}

	public void close() {
		CloseUtil.close(inputStream);
	}

	public ByteBuf getByteBuf() {
		return buf;
	}

	public String getFileName() {
		return fileName;
	}
	
	public String getTqName() {
		return tqName;
	}

	public String getTqPrefix() {
		return tqPrefix;
	}
}
