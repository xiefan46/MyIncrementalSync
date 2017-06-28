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
package com.alibaba.middleware.race.sync.util;

import java.util.BitSet;

/**
 * @author wangkai
 *
 */
public class RecordMap2 {

	private int	cols;

	private int	recordLen;

	private byte[]	data;

	private int	capacity;

	private int	off;

	private BitSet hasValue;

	public RecordMap2(int capacity, int off, int cols) {
		this.off = off;
		this.cols = cols;
		this.capacity = capacity;
		this.init(capacity, cols);
	}

	private void init(int capacity, int cols) {
		this.recordLen = 8 * cols;
		this.data = new byte[capacity * recordLen];
		hasValue = new BitSet(capacity);
	}

	public int getResult(int pk) {
		int idx = ix(pk);
		if(hasValue.get(ix(pk)))
			return idx * recordLen;
		return -1;
	}

	public void add(int pk){
		hasValue.set(ix(pk),true);
	}

	public void delete(int pk){
		hasValue.set(ix(pk),false);
	}


	private int ix(int pk) {
		return pk - off;
	}


	public void setColumn(int pk, byte name, short version, byte[] src, int off, int len) {
		int iPk = ix(pk);
		int vI = iPk * cols + name;
		int tOff = name * 8 + iPk * recordLen;
		byte[] target = data;
		target[tOff++] = (byte) len;
		int end = off + len;
		for (int i = off; i < end; i++) {
			target[tOff++] = src[i];
		}
	}

	/**
	 * @return the data
	 */
	public byte[] getData() {
		return data;
	}

}
