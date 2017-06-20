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
package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class FullColumnLog extends ColumnLog {

	private byte[]	nameBytes;

	private int	i;

	public FullColumnLog(int i) {
		this.i = i;
	}

	@Override
	public void setName(Table table, byte[] bytes, int off, int len) {
		this.nameBytes = byteArray2.reset(bytes, off, len).getBytes();
		this.setName(i);
	}

	public byte[] getNameBytes() {
		return nameBytes;
	}

}
