package com.alibaba.middleware.race.sync;

import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;

/**
 * Created by xiefan on 6/11/17.
 */
public class PageCacheHelperTest {
	@Test
	public void test() throws Exception {
		PageCacheHelper helper = new PageCacheHelper();
		helper.readSingleThread();
	}

	@Test
	public void test2() throws Exception {
		PageCacheHelper helper = new PageCacheHelper();
		helper.readMultiThread();
	}

	@Test
	public void testRead() throws Exception {
		File root = new File(Constants.DATA_HOME);
		long start = System.currentTimeMillis();
		byte[] bytes = new byte[4096];
		int flag = 1;
		int count = 0;
		for (File f : root.listFiles()) {
			System.out.println("file name : " + f.getName());
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
			int size = bis.read(bytes);
			while (size != -1) {
				size = bis.read(bytes);
				flag |= size;
				if (size <= 0) {
					System.out.println("size : " + size);
				}
			}
			count++;
			System.out.println(count);
			bis.close();
		}
		System.out.println("time : " + (System.currentTimeMillis() - start));
		System.out.println(flag);
	}
}
