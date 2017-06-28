package test;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Context;
import com.alibaba.middleware.race.sync.JvmUsingState;
import com.alibaba.middleware.race.sync.util.RecordUtil;
import com.generallycloud.baseio.common.FileUtil;
import com.generallycloud.baseio.common.ThreadUtil;

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
//		ThreadUtil.execute(new PageCacheHelper());
		String schema = "middleware3";
		String table = "student";
		int startId = 1000000;
		int endId = 8000000;
//		startId = 170000;
//		endId = 170100;
		ThreadUtil.execute(new JvmUsingState());
		Context context = new Context(endId, startId);
		context.initialize();
		context.getMainThread().execute();
		
		//194120
		RecordUtil.writeResultToLocalFile(context, Constants.RESULT_HOME + "/" +Constants.RESULT_FILE_NAME);
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
