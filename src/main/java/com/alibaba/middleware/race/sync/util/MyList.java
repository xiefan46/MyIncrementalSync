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

/**
 * @author wangkai
 *
 */
public class MyList<V> {
	
	private V [] vs;
	
	private int capacity;
	
	private int pos;
	
	public MyList(int capacity) {
		this.capacity = capacity;
		this.vs = (V[]) new Object[capacity];
	}

	public void add(V v){
		vs[pos++] = v;
	}
	
	public V get(int index){
		return vs[index];
	}
	
	public void clear(){
		pos = 0;
	}
	
	/**
	 * @return the pos
	 */
	public int getPos() {
		return pos;
	}
	
	public V get(){
		return vs[pos++];
	}

}
