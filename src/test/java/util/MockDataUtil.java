package util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * Created by xiefan on 5/30/17.
 */

/*
 * schema ； middleware table : user field: id long, pk name str score long
 */
public class MockDataUtil {
	private static final long	seed				= 1234;

	public static Random		rand				= new Random(seed);;

	private static char[]		numbersAndLetters	= ("0123456789abcdefghijklmnopqrstuvwxyz"
			+ "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").toCharArray();

	private static final String	beginDate			= "2010-09-01";

	private static final String	endDate			= "2017-07-01";

	private static final String	FIELD_SEPERATOR_1	= "|";

	private static final String	FIELD_SEPERATOR_2	= ":";

	private static final int		IS_NUMBER			= 1;

	private static final int		IS_STRING			= 2;

	private static final int		IS_PK			= 1;

	private static final int		NOT_PK			= 0;

	private static final int		ID_LENGTH			= 10;

	public static String getRamdonString(int length) {
		char[] randBuffer = new char[length];
		for (int i = 0; i < length; i++) {
			randBuffer[i] = numbersAndLetters[rand.nextInt(numbersAndLetters.length)];
		}
		return new String(randBuffer);
	}

	public static long getRandomTime() throws Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date start = format.parse(beginDate);
		Date end = format.parse(endDate);
		if (start.getTime() >= end.getTime()) {
			return -1;
		}
		Float f = new Float(rand.nextFloat() * (end.getTime() - start.getTime()));
		long result = start.getTime() + f.longValue();
		return result;
	}

	public static String getUUID() {
		UUID uuid = UUID.randomUUID();
		String str = uuid.toString();
		return str;
	}

	public static String mockInsertLog() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(mockLogHeader() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.INSERT + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getIdDesc() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.NULL_FIELD + FIELD_SEPERATOR_1);
		sb.append(randPositiveLong() + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getNameDesc() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.NULL_FIELD + FIELD_SEPERATOR_1);
		sb.append(getRamdonString(20) + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getScoreDesc() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.NULL_FIELD + FIELD_SEPERATOR_1);
		sb.append(randPositiveLong());
		sb.append(FIELD_SEPERATOR_1);
		return sb.toString();
	}

	/*
	 * 目前只生成模拟name字段update的log
	 */
	public static String mockUpdateLog() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(mockLogHeader() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.UPDATE + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getIdDesc() + FIELD_SEPERATOR_1);
		long id = randPositiveLong();
		sb.append(id + FIELD_SEPERATOR_1);
		sb.append(id + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getNameDesc() + FIELD_SEPERATOR_1);
		sb.append(getRamdonString(10) + FIELD_SEPERATOR_1);
		sb.append(getRamdonString(10));
		sb.append(FIELD_SEPERATOR_1);
		return sb.toString();
	}

	public static String mockDeleteLog() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(mockLogHeader() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.DELETE + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getIdDesc() + FIELD_SEPERATOR_1);
		sb.append(randPositiveLong() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.NULL_FIELD + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getNameDesc() + FIELD_SEPERATOR_1);
		sb.append(getRamdonString(20) + FIELD_SEPERATOR_1);
		sb.append(TestConstant.NULL_FIELD + FIELD_SEPERATOR_1);
		sb.append(UserRecord.getScoreDesc() + FIELD_SEPERATOR_1);
		sb.append(randPositiveLong() + FIELD_SEPERATOR_1);
		sb.append(TestConstant.NULL_FIELD);
		sb.append(FIELD_SEPERATOR_1);
		return sb.toString();
	}

	private static String mockLogHeader() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(FIELD_SEPERATOR_1);
		sb.append(getUUID() + FIELD_SEPERATOR_1);
		sb.append(getRandomTime() + FIELD_SEPERATOR_1);
		sb.append(UserRecord.SCHEMA + FIELD_SEPERATOR_1);
		sb.append(UserRecord.TABLE);
		return sb.toString();
	}

	private static long randPositiveLong() {
		return Math.abs(rand.nextLong());
	}

	public static class UserRecord {

		public static final String	ID		= "id";

		public static final String	NAME		= "name";

		public static final String	SCORE	= "score";

		public static final String	SCHEMA	= "middleware";

		public static final String	TABLE	= "user";

		int						id;

		String					name;

		int						score;

		public static String getIdDesc() {
			return ID + FIELD_SEPERATOR_2 + IS_NUMBER + FIELD_SEPERATOR_2 + IS_PK;
		}

		public static String getNameDesc() {
			return NAME + FIELD_SEPERATOR_2 + IS_STRING + FIELD_SEPERATOR_2 + NOT_PK;
		}

		public static String getScoreDesc() {
			return SCORE + FIELD_SEPERATOR_2 + IS_NUMBER + FIELD_SEPERATOR_2 + NOT_PK;
		}
	}

}
