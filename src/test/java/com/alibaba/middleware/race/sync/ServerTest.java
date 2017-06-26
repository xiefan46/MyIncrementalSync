package com.alibaba.middleware.race.sync;

import org.junit.Test;

/**
 * Created by xiefan on 6/27/17.
 */
public class ServerTest {
	@Test
	public void test() throws Exception {
		String[] args = new String[] { "middleware5", "student", "100000", "2000000" };
		Server server = new Server();
		server.main(args);
	}
}
