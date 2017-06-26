package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;

import com.alibaba.middleware.race.sync.channel.MuiltFileInputStream;
import com.alibaba.middleware.race.sync.channel.MuiltFileReadChannelSplitor;
import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

public class TestCountLines {

	
	public static void main(String[] args) throws Exception {
		
		int all = 0;
		
		ByteBuf buf = UnpooledByteBufAllocator.getHeapInstance().allocate(1024 * 1024 * 4);
		
		MuiltFileInputStream inputStream = initChannels2();
		for(;inputStream.hasRemaining();){
			buf.clear();
			inputStream.readFull(buf, buf.capacity());
			buf.flip();
			for (int i = 0; i < buf.limit(); i++) {
				if (buf.getByte() == '\n') {
					all++;
					if (all % 10000000 == 0) {
						System.out.println(all);
					}
				}
			}
		}
		
		System.out.println(all);
	}
	
	
	private static MuiltFileInputStream initChannels2() throws IOException {
		File root = new File(Constants.DATA_HOME);
		return MuiltFileReadChannelSplitor.newInputStream(root.getAbsolutePath() + "/", 1, 10,
				1024 * 256);
	}
}
