package com.alibaba.middleware.race.sync.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by xiefan on 6/5/17.
 */
public class Block {

	int				fileOffset;

	RandomAccessFile	raf;

	long				start;

	long				end;

	public Block(int fileOffset, RandomAccessFile raf, long start, long end) {
		this.fileOffset = fileOffset;
		this.raf = raf;
		this.start = start;
		this.end = end;
	}

	public MappedByteBuffer getReadOnlyMbb() throws IOException {
		MappedByteBuffer mbb;
		return raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, end - start + 1);
	}

	@Override
	public String toString() {
		return "file offset : " + fileOffset + " start : " + start + " end : " + end;
	}

	public int getFileOffset() {
		return fileOffset;
	}

	public void setFileOffset(int fileOffset) {
		this.fileOffset = fileOffset;
	}

	public RandomAccessFile getRaf() {
		return raf;
	}

	public void setRaf(RandomAccessFile raf) {
		this.raf = raf;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}
}
