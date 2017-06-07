package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.util.RecordUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.generallycloud.baseio.common.FileUtil;

import util.MockDataUtil;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by xiefan on 6/4/17.
 */
public class MainThreadTest {

	File	dataDir	= new File(Constants.DATA_HOME);

	File	middleDir	= new File(Constants.MIDDLE_HOME);

	File	resultDir	= new File(Constants.RESULT_HOME);

	int	fileNum	= 10;

	@Before
	public void before() throws Exception {
		cleanUpAll();
	}

	@Ignore
	@Test
	public void createTestData() throws Exception {
		cleanDir(dataDir);
		for (int i = 0; i < fileNum; i++) {
			BufferedOutputStream bos = null;
			try {
				bos = new BufferedOutputStream(
						new FileOutputStream(Constants.DATA_HOME + "/" + i + ".txt"));
				for (int j = 0; j < 10; j++) { //每个文件生成10条insert语句
					String mockInsert = MockDataUtil.mockInsertLog();
					mockInsert += '\n';
					bos.write(mockInsert.getBytes());
				}
				bos.flush();
			} finally {
				if (bos != null)
					bos.close();
			}
		}
	}

	@Test
	public void testBasic() throws Exception {
		RecordLogReceiver recordLogReceiver = new RecordLogReceiverImpl();
		String schema = "middleware3";
		String table = "student";
		long startId = 0;
		long endId = Long.MAX_VALUE;
		MainThread mainThread = new MainThread(recordLogReceiver, schema, table, startId, endId);
		Thread t = new Thread(mainThread);
		t.start();
		t.join();
		RecordUtil.writeResultToLocalFile(mainThread.getFinalContext(),
				Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME);
	}

	@After
	public void after() throws Exception {

	}

	private void cleanUpAll() throws Exception {
		cleanDir(resultDir);
		cleanDir(middleDir);
	}

	private void cleanDir(File dir) throws Exception {
		if (dir.exists()) {
			FileUtil.cleanDirectory(dir);
			return;
		}
		FileUtil.createDirectory(dir);
	}

}
