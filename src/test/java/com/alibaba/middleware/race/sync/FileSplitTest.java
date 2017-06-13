package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.Constants;
import com.generallycloud.baseio.common.FileUtil;
import org.junit.Before;
import org.junit.Test;
import util.MockDataUtil;

import java.io.File;
import java.io.IOException;

/**
 * Created by xiefan on 6/6/17.
 */
public class FileSplitTest {

	private String	inputFilePath	= "/home/admin/canal.txt";

	private String	outputDir		= Constants.DATA_HOME;
	@Before
	public void before() throws IOException {
		File dir = new File(outputDir);
		if (dir.exists()) {
			FileUtil.cleanDirectory(dir);
		} else {
			FileUtil.createDirectory(dir);
		}
	}

	@Test
	public void test() throws IOException {
		MockDataUtil.fileSplit(inputFilePath, outputDir, 10);
	}
}
