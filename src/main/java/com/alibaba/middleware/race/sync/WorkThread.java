package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WorkThread extends Thread {

	private static Logger	logger	= LoggerFactory.getLogger(WorkThread.class);

	private Object			lock		= new Object();

	private volatile boolean	isRunning	= true;

	private boolean		work;

	private int			index;

	public WorkThread(String threadName, int index) {
		super(threadName + index);
		this.index = index;
	}

	@Override
	public void run() {
		for (;;) {
			if (!work) {
				wait4Work();
			}
			if (!isRunning) {
				break;
			}
			try {
				work();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}finally{
				work = false;
			}
		}
	}

	protected abstract void work() throws Exception;
	
	public boolean isRunning() {
		return isRunning;
	}

	public boolean isWork() {
		return work;
	}
	
	/**
	 * @param work the work to set
	 */
	public void setWork(boolean work) {
		this.work = work;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	public void wakeup() {
		synchronized (lock) {
			lock.notify();
		}
	}
	
	public void startWork(){
		this.work = true;
		this.wakeup();
	}

	private void wait4Work() {
		synchronized (lock) {
			try {
				lock.wait();
			} catch (Throwable e) {
				logger.error(e.getMessage(), e);
			}
		}
	}
	
	public int getIndex() {
		return index;
	}
	
	public void shutdown(){
		this.isRunning = false;
		this.wakeup();
	}
	
}