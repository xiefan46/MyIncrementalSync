package mock;

import org.junit.Test;
import util.MockDataUtil;

/**
 * Created by xiefan on 5/30/17.
 */

public class MockTestData {

	@Test
	public void printLog() throws Exception {
		System.out.println(MockDataUtil.mockInsertLog());
		System.out.println(MockDataUtil.mockUpdateLog());
		System.out.println(MockDataUtil.mockDeleteLog());
	}

	@Test
	public void test2() throws Exception {
		byte update = 'I';
		char update2 = 'I';
		System.out.println(update == update2);
	}
}
