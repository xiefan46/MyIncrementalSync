package com.alibaba.middleware.race.sync.model.result;

import com.alibaba.middleware.race.sync.BufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiefan on 6/24/17.
 */
public class ParseResult {

	private List<ByteBuffer>	list	= new ArrayList<>();

	private int			id;

	private BufferPool pool;

	public ParseResult(int id,BufferPool pool) {
		this.id = id;
		this.pool = pool;
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

	public BufferPool getPool() {
		return pool;
	}

	public void setPool(BufferPool pool) {
		this.pool = pool;
	}
}
