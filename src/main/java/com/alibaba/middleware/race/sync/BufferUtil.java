package com.alibaba.middleware.race.sync;

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * Created by xiefan on 6/23/17.
 */
public class BufferUtil {

	public static final int			MAX_BLOCK_NUM	= 20;								//同时存在于内存的块数

	public static final int			BLOCK_SIZE	= 512 * 1024;

	private static ByteBufAllocator	allocator		= UnpooledByteBufAllocator.getHeapInstance();

	public static ByteBuf getOneBlock() {
		return allocator.allocate(BLOCK_SIZE);
	}
}
