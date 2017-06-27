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

import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 *
 */
public class RecordMap {

	private Record[]	vs;

	private int	capacity;

	private int	off;

	public RecordMap(int capacity, int off,int cols) {
		this.off = off;
		this.capacity = capacity;
		this.vs = init(capacity, cols);
	}
	
	private Record[] init(int capacity,int cols){
		Record[] vs = new Record[capacity];
		for (int i = 0; i < capacity; i++) {
			vs[i] = Record.newRecord(cols);
		}
		return vs;
	}

	public Record get(int index) {
		return vs[index - off];
	}

}
