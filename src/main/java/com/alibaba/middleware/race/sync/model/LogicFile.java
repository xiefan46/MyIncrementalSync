package com.alibaba.middleware.race.sync.model;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.sync.stream.LogicFileInputStream;
import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * Created by xiefan on 6/5/17.
 */
public class LogicFile {

	private static final Logger	logger	= LoggerFactory.getLogger(LogicFile.class);

	List<Block>				blocks;

	public LogicFile(List<Block> blocks) {
		this.blocks = blocks;
	}

	public LogicFileInputStream getInputStream() throws IOException {
		List<MappedByteBuffer> mbbs = new ArrayList<>();
		for (Block b : blocks) {
			mbbs.add(b.getReadOnlyMbb());
		}
		return new LogicFileInputStream(mbbs);
	}

	public void dump() {
		logger.info("------------dump LogicFile---------------");
		logger.info("block count : " + blocks.size());
		for (Block b : blocks) {
			logger.info(b.toString());
		}
		logger.info("----------------------------");
	}
}
