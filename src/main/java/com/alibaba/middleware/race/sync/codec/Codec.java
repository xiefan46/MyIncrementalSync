package com.alibaba.middleware.race.sync.codec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.Column;
import com.alibaba.middleware.race.sync.model.Record;
import com.generallycloud.baseio.common.MathUtil;

/**
 * @author wangkai
 *
 */
public class Codec {

	private final byte[] array = new byte[1024 * 128];
	private final ByteBuffer byteBuffer = ByteBuffer.wrap(array);
	
	private final static CharsetEncoder ENCODER = Charset.defaultCharset().newEncoder();

	public Record decode(byte[] data, int offset, int len) {
		throw new UnsupportedOperationException();
	}

	public int encode(Record record) {
		byte[] array = this.array;
		StringCache cache = StringCache.get();
		byte[] cs = cache.getCache(record.getTableSchema());
		array[4] = cs[0];
		array[5] = cs[1];
		int off = encode(record.getPrimaryColumn(),array,6);
		Map<String, Column> cls = record.getColumns();
		if (cls == null) {
			return off;
		}
		for(Column c : cls.values()){
			off = encode(c, array, off);
		}
		return off;
	}

	private int encode(Column c,byte [] array,int off) {
		StringCache cache = StringCache.get();
		byte[] cs = cache.getCache(c.getName());
		array[off++] = cs[0];
		array[off++] = cs[1];
		array[off++] = c.getFlag();
		if (c.isNumber()) {
			MathUtil.unsignedInt2Byte(array, (long)c.getValue(), off);
			return off + 4;
		}
		int flag = off;
		String v = (String)c.getValue();
		ByteBuffer temp = this.byteBuffer;
		CharsetEncoder ENCODER = Codec.ENCODER;
		temp.clear().position(off+2);
		CoderResult cr = ENCODER.encode(CharBuffer.wrap(v.toCharArray()), temp, true);
		if (cr.isError()) {
			try {
				cr.throwException();
			} catch (CharacterCodingException e) {
				throw new RuntimeException(e);
			}
		}
		ENCODER.reset();
		int len = temp.position() - flag - 2;
		MathUtil.unsignedShort2Byte(array, len, off);
		return temp.position();
	}

	private long byte2ShortLong(byte[] bytes, int offset) {
		long v0 = bytes[offset + 5] & 0xff;
		long v1 = (long) (bytes[offset + 4] & 0xff) << 8 * 1;
		long v2 = (long) (bytes[offset + 3] & 0xff) << 8 * 2;
		long v3 = (long) (bytes[offset + 2] & 0xff) << 8 * 3;
		long v4 = (long) (bytes[offset + 1] & 0xff) << 8 * 4;
		long v5 = (long) (bytes[offset + 0] & 0xff) << 8 * 5;
		return (v0 | v1 | v2 | v3 | v4 | v5);
	}

	private void shortLong2Byte(byte[] bytes, long value, int offset) {
		bytes[offset + 5] = (byte) (value);
		bytes[offset + 4] = (byte) (value >> 8 * 1);
		bytes[offset + 3] = (byte) (value >> 8 * 2);
		bytes[offset + 2] = (byte) (value >> 8 * 3);
		bytes[offset + 1] = (byte) (value >> 8 * 4);
		bytes[offset + 0] = (byte) (value >> 8 * 5);
	}

	public byte[] getArray() {
		return array;
	}

}
