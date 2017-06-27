package com.alibaba.middleware.race.sync;

/**
 * 外部赛示例代码需要的常量 Created by wanshao on 2017/5/25.
 */
public interface Constants {

	// ------------ 本地测试可以使用自己的路径--------------//

	// 工作主目录
	// String TESTER_HOME = "/Users/wanshao/work/middlewareTester";
	// 赛题数据
	String	DATA_HOME		= "/home/admin/canal_data";
	// teamCode
	String	TEAMCODE		= "737049rihy";
	// 结果文件目录
	String	RESULT_HOME	= "/home/admin/sync_results/" + TEAMCODE;
	// 日志级别
	String	LOG_LEVEL		= "INFO";
	// 中间结果目录
	String	MIDDLE_HOME	= "/home/admin/middle/" + TEAMCODE;
	// server端口
	Integer	SERVER_PORT	= 5527;

	// ------------ 正式比赛指定的路径--------------//
	//// 工作主目录
	String	TESTER_HOME	= "/home/admin";

	String	RESULT_FILE	= "/home/admin/sync_results/" + TEAMCODE + "/Result.rs";

	byte		INSERT		= 'I';

	byte		UPDATE		= 'U';

	byte		PK_UPDATE		= 'P';

	byte		DELETE		= 'D';

	byte		WAIT			= 5;

	byte		END			= -1;

	int		RESULT_LENGTH	= 38334025;

}
