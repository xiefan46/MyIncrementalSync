package com.alibaba.middleware.race.sync;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import com.generallycloud.baseio.buffer.ByteBuf;

/**
 * @author wangkai
 *
 */
public class CompoundReadChannelSplitor {

	public static CompoundReadChannel [] split(File root,int size) throws IOException{
		File[] fs = root.listFiles();
		FilesManager manager = createFilesManager(fs);
		long all = root.length();
		long unit = all / size;
		long remainder = all % size;
		CompoundReadChannel [] cs = new CompoundReadChannel[size];
		for (int i = 0,csAll = cs.length -1; i < csAll; i++) {
			List<InputStream> inputStreams = selectInputStreams(manager, unit);
			cs[i] = new CompoundReadChannel(inputStreams, 1024 * 128 , unit); 
			if (i == 0) {
				continue;
			}
		}
		long finalUnit = unit + remainder;
		List<InputStream> inputStreams = selectInputStreams(manager, finalUnit);
		cs[cs.length-1] = new CompoundReadChannel(inputStreams, 1024 * 128 , finalUnit);
		handleChannelTail(cs);
		return cs;
	}
	
	private static void handleChannelTail(CompoundReadChannel [] cs) throws IOException{
		for (int i = 1; i < cs.length; i++) {
			CompoundReadChannel c = cs[i];
			ByteBuf buf = c.getByteBuf();
			c.read(buf);
			int off = findNextChar(buf.array(), 0, buf.limit(), '\n');
			if (off == -1) {
				throw new IOException("too big nnnnnn...");
			}
			buf.position(off+1);
			byte [] tail = new byte[off];
			System.arraycopy(buf.array(), 0, tail, 0, off);
			cs[i-1].setTail(tail);
		}
	}
	
	private static int findNextChar(byte[] data, int offset, int end, char c) {
		for (;;) {
			if (data[offset] == c) {
				return offset;
			}
			if (++offset == end) {
				return -1;
			}
		}
	}
	
	private static FilesManager createFilesManager(File [] fs){
		FileHolder [] fh = new FileHolder[fs.length];
		for (int i = 0; i < fs.length; i++) {
			fh[i] = new FileHolder(fs[i]);
		}
		return new FilesManager(fh);
	}
	
	private static List<InputStream> selectInputStreams(FilesManager manager, long len) throws IOException{
		List<InputStream> streams = new ArrayList<>();
		long _len = len;
		for(;;){
			FileHolder fh = manager.getFs()[manager.getCurrentIndex()];
			File file = new File(fh.getFile().getAbsolutePath());
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			if (fh.getOffset() > 0) {
				raf.seek(fh.getOffset());
			}
			RAFInputStream inputStream = new RAFInputStream(raf);
			long remaining = fh.getLength() - fh.getOffset();
			if (remaining > _len) {
				fh.setOffset(remaining - _len);
				streams.add(inputStream);
				return streams;
			}else{
				_len -= remaining;
				manager.setCurrentIndex(manager.getCurrentIndex()+1);
				streams.add(inputStream);
			}
		}
	}
	
	static class FileHolder{
		
		private File file;
		private long offset;
		private long length;
		
		public FileHolder(File file) {
			this.file = file;
			this.length = file.length();
		}
		public File getFile() {
			return file;
		}
		public void setFile(File file) {
			this.file = file;
		}
		public long getOffset() {
			return offset;
		}
		public void setOffset(long offset) {
			this.offset = offset;
		}
		public long getLength() {
			return length;
		}
	}
	
	static class FilesManager{
		
		public FilesManager(FileHolder[] fs) {
			this.fs = fs;
		}
		private FileHolder[] fs;
		private int currentIndex;
		public FileHolder[] getFs() {
			return fs;
		}
		public void setFs(FileHolder[] fs) {
			this.fs = fs;
		}
		public int getCurrentIndex() {
			return currentIndex;
		}
		public void setCurrentIndex(int currentIndex) {
			this.currentIndex = currentIndex;
		}
	}
	
	
}
