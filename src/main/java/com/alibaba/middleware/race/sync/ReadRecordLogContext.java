package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.channel.ReadChannel;

/**
 * @author wangkai
 */
public class ReadRecordLogContext {

	private ReadChannel	channel;
	private Context	context;

	public ReadRecordLogContext(ReadChannel channel, Context context) {
		super();
		this.channel = channel;
		this.context = context;
	}

	public ReadChannel getChannel() {
		return channel;
	}

	public void initialize() {

	}

	public Context getContext() {
		return context;
	}

}
