package com.alibaba.middleware.race.sync.codec;

/**
 * @author wangkai
 *
 */
public class CodecUtil {

	public static int findNextChar(byte[] data, int offset, char c) {
		for (;;) {
			if (data[++offset] == c) {
				return offset;
			}
		}
	}
	
}
