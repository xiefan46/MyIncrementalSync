package com.alibaba.middleware.race.sync.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.sync.Client;
import com.alibaba.middleware.race.sync.Server;

/**
 * @author wangkai
 *
 */
public class LoggerUtil {

	public static Logger SERVER_LOGGER = LoggerFactory.getLogger(Server.class);
	
	public static Logger CLIENT_LOGGER = LoggerFactory.getLogger(Client.class);
}
