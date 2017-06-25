/*
 * Copyright 2015-2017 GenerallyCloud.com
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.middleware.race.sync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.generallycloud.baseio.buffer.ByteBuf;
import com.generallycloud.baseio.buffer.ByteBufAllocator;
import com.generallycloud.baseio.buffer.UnpooledByteBufAllocator;

/**
 * @author wangkai
 *
 */
public class ByteBufPool {

	private ArrayBlockingQueue<ByteBuf> bufs;

	public ByteBufPool(int capacity,int unit) {
		this.bufs = new ArrayBlockingQueue<>(capacity);
		ByteBufAllocator allocator = UnpooledByteBufAllocator.getHeapInstance();
		for (int i = 0; i < capacity; i++) {
			bufs.offer(allocator.allocate(unit));
		}
	}
	
	public ByteBuf allocate(){
		try {
			return bufs.poll(16, TimeUnit.MICROSECONDS);
		} catch (InterruptedException e) {
			return null;
		}
	}
	
	public void free(ByteBuf buf){
		buf.clear();
		bufs.offer(buf);
	}
	
	
}
