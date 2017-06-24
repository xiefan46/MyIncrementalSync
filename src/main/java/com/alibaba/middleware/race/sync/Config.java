package com.alibaba.middleware.race.sync;

/**
 * Created by xiefan on 6/24/17.
 */
public final class Config {


	public static final int		READ_BUFFER_SIZE				= Integer
			.valueOf(System.getProperty("read.buffer.size", "" + (1 << 20)));

	//分区相关的配置


	public static final int		INRANGE_REPLAYER_COUNT			= Integer
			.valueOf(System.getProperty("inrange.replayer.count", "128"));					//查询区间内的分区数,采用Range Partition

	// 查询区间内的分区大小设定
	public static final int		INRANGE_PARTITION_BUFFER_COUNT	= INRANGE_REPLAYER_COUNT * 4;
	public static final int		INRANGE_PARTITION_BUFFER_SIZE		= READ_BUFFER_SIZE * 2
			/ INRANGE_PARTITION_BUFFER_COUNT;

	public static final int		OUTRANGE_REPLAYER_COUNT			= Integer
			.valueOf(System.getProperty("outrange.replayer.count", "4"));

	// 查询区间外的分区大小设定
	public static final int		OUTRANGE_PARTITION_BUFFER_COUNT	= OUTRANGE_REPLAYER_COUNT * 4;

	public static final int		OUTRANGE_PARTITION_BUFFER_SIZE	= READ_BUFFER_SIZE * 2
			/ OUTRANGE_PARTITION_BUFFER_COUNT;

}
