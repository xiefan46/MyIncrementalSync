package com.alibaba.middleware.race.sync;

/**
 * Created by xiefan on 6/27/17.
 */
public interface Config {

	boolean	DEBUG			= false;

	int		BLOCK_BUFFER_COUNT	= 128;

	int		PARSER_COUNT		= 12;

	int		CALCULATOR_COUNT	= 32;

	int		BLOCK_BUFFER_SIZE	= 1024 * 1024;

	int		RECORD_BUFFER_COUNT	= CALCULATOR_COUNT * PARSER_COUNT * 4;

	int		RECORD_BUFFER_SIZE	= BLOCK_BUFFER_SIZE * 4 / RECORD_BUFFER_COUNT;

}
