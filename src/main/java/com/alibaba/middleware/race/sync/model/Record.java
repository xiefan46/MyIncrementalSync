package com.alibaba.middleware.race.sync.model;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Record {

	private byte [] columns;
	
	private AtomicInteger power = new AtomicInteger(0);
	
	private AtomicBoolean lock = new AtomicBoolean();
	
	private int [] versions;

	public int getPower() {
		return power.get();
	}

	public AtomicBoolean getLock() {
		return lock;
	}
	
	public byte[] getColumns() {
		return columns;
	}

	public void powerIncrement() {
		this.power.incrementAndGet();
	}
	
	public void powerDecrement() {
		this.power.decrementAndGet();
	}

	public void lockRecord(){
		AtomicBoolean lock = this.lock;
		if (!lock.compareAndSet(false, true)) {
			for(;lock.compareAndSet(false, true);){
			}
		}
	}
	
	public void newColumns(int cols) {
		this.columns = new byte[cols * 8];
		this.versions = new int[cols];
	}
	
	public static Record newRecord(int cols){
		Record r = new Record();
		r.newColumns(cols);
		return r;
	}
	
	public void releaseRecordLock(){
		lock.set(false);
	}
	
	public void setColumn(byte name,int version, byte[] src, int off, int len){
		if (version < versions[name]) {
			return;
		}
		versions[name] = version;
		byte [] target = this.columns;
		int tOff = name * 8;
		target[tOff++] = (byte)len;
		int end = off + len;
		for (int i = off; i < end; i++) {
			target[tOff++] = src[i];
		}
	}
	
}
