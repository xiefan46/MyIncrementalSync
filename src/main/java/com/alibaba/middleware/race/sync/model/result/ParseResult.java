package com.alibaba.middleware.race.sync.model.result;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.sync.common.BufferPool;

/**
 * Created by xiefan on 6/24/17.
 */
public class ParseResult {

	private List<ByteBuffer>	list	= new ArrayList<>();

	private int			id;

	public ParseResult(int id) {
		this.id = id;
	}

	public List<ByteBuffer> getList() {
		return list;
	}

	public void addBuffer(ByteBuffer buffer) {
		list.add(buffer);
	}

	public int getId() {
		return id;
	}
}
