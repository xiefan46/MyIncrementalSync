package com.alibaba.middleware.race.sync;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.middleware.race.sync.util.RecordUtil;
import com.generallycloud.baseio.common.FileUtil;

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

	@Test
	public void testBasic() throws Exception {
		RecordLogReceiver recordLogReceiver = new RecordLogReceiverImpl();
		String schema = "middleware3";
		String table = "student";
		long startId = 600;
		long endId = 700;
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
