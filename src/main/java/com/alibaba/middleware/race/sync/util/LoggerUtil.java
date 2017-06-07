package com.alibaba.middleware.race.sync.util;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiefan on 6/8/17.
 */
public class LoggerUtil {
	public static Logger getServerLogger() {
		return LoggerFactory.getLogger(Server.class);
	}
}
