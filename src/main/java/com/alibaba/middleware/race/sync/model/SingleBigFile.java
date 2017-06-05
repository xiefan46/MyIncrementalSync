package com.alibaba.middleware.race.sync.model;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.generallycloud.baseio.common.Logger;
import com.generallycloud.baseio.common.LoggerFactory;

/**
 * Created by xiefan on 6/5/17.
 */
/*
 * 把所有输入文件从逻辑上看成一个大文件，然后再根据线程数目或者其他东西对文件从逻辑删进行划分
 */
public class SingleBigFile {
    
	private List<File>			files;

	private List<FileChannel>	channels;

	private List<LogicFile>		logicFiles;

	private long				totalLength	= 0;

	private static final Logger	logger		= LoggerFactory.getLogger(SingleBigFile.class);

	public SingleBigFile(List<File> files) {
		this.files = files;
		Collections.sort(this.files);
	}

	/*
	 * 先按大小大致划分，再按\n给前面的分块补足一行
	 */
	public void split(int logicFileNum) throws IOException {
		logicFiles = new ArrayList<>();
		List<RandomAccessFile> rafs = new ArrayList<>();
		for (File f : files) {
			totalLength += f.length();
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			rafs.add(raf);
			channels.add(raf.getChannel());
		}
		long blockSizeEstimate = totalLength / logicFileNum;
		int fileOffset = 0;
		long start = 0;
		long needSize = blockSizeEstimate;
		RandomAccessFile curRaf = rafs.get(fileOffset);
		long fileRemain = curRaf.length();
		List<Block> blocks = new ArrayList<>();
		while (true) {
			if (fileRemain >= needSize) {
				long end = findNextRecord(curRaf, start + needSize) - 1;
				fileRemain -= (end - start + 1);
				blocks.add(new Block(fileOffset, curRaf, start, end));
				logicFiles.add(new LogicFile(blocks));
				blocks = new ArrayList<>();
			} else {
				blocks.add(new Block(fileOffset, curRaf, start, start + fileRemain - 1));
				needSize -= fileRemain;
				fileOffset++;
				if (fileOffset >= files.size())
					break;
				curRaf = rafs.get(fileOffset);
				start = 0;
				fileRemain = curRaf.length();
			}
		}

		if (!blocks.isEmpty()) { //补足最后一块
			logicFiles.add(new LogicFile(blocks));
			blocks.clear();
		}
		checkState(logicFiles.size() == logicFileNum, "得到的逻辑划分不满足需要的文件数要求");
	}

	public List<LogicFile> getLogicFiles() {
		return logicFiles;
	}

	public void close() throws IOException {
		for (FileChannel fc : channels) {
			if (fc != null)
				fc.close();
		}
	}

	private long findNextRecord(RandomAccessFile raf, long offset) throws IOException {
		raf.seek(offset);
		for (;;) {
			if (raf.readByte() == '\n') {
				return raf.getFilePointer();
			}
		}
	}

}
