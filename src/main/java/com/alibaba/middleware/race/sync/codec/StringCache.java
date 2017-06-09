package com.alibaba.middleware.race.sync.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.generallycloud.baseio.common.CloseUtil;
import com.generallycloud.baseio.common.FileUtil;
import com.generallycloud.baseio.common.MathUtil;
import com.generallycloud.baseio.component.ByteArrayBuffer;

/**
 * @author wangkai
 */
public class StringCache {

	private static StringCache stringCache = new StringCache();

	public static StringCache get() {
		return stringCache;
	}

	private boolean			initialized		= false;

	private int				cacheHash			= 1;

	private int				MAX_MAP_SZIE		= (1 << 15);

	private final CharsetEncoder	CHARSET_ENCODER	= Charset.defaultCharset().newEncoder();

	public static final byte	CACHE_FLAG		= (byte) 0B10000000;

	private Map<String, byte[]>	cacheStringMap		= new HashMap<>();

	private Map<Integer, String>	cacheIntegerMap	= new HashMap<>();

	public String getCache(Integer key) {
		return cacheIntegerMap.get(key);
	}

	public void initialize(InputStream inputStream) throws IOException {
		if (inputStream == null) {
			return;
		}
		synchronized (this) {
			if (initialized) {
				return;
			}
			initialized = true;
			byte[] bb = new byte[4];
			inputStream.read(bb);
			int len = MathUtil.byte2Int(bb, 0);
			byte[] array = new byte[len];
			FileUtil.readInputStream(inputStream, array);
			decode(array, 0, len);
		}
	}

	private void decode(byte[] array, int off, int len) {
		for (;;) {
			if (off >= len) {
				break;
			}
			int kl = array[off++];
			String k = new String(array, off, kl);
			off += kl;
			byte[] hashArray = new byte[2];
			hashArray[0] = array[off++];
			hashArray[1] = array[off++];
			int hash = ((hashArray[0] & 0b01111111) << 16) | (hashArray[1] & 0xff);
			if (cacheStringMap.containsKey(k)) {
				continue;
			}
			cacheStringMap.put(k, hashArray);
			cacheIntegerMap.put(Integer.valueOf(hash), k);
		}
	}

	public void save(OutputStream outputStream) throws IOException {
		byte[] temp = new byte[40];
		ByteArrayBuffer out = new ByteArrayBuffer(new byte[1024 * 4],4);
		ByteBuffer buffer = ByteBuffer.wrap(temp);
		int all = 0;
		for (Entry<String, byte[]> e : cacheStringMap.entrySet()) {
			int off = 0;
			String k = e.getKey();
			temp[off++] = (byte) k.length();
			CharsetEncoder CHARSET_ENCODER = this.CHARSET_ENCODER;
			CHARSET_ENCODER.reset();
			buffer.clear().position(1);
			CoderResult cr = CHARSET_ENCODER.encode(CharBuffer.wrap(k.toCharArray()), buffer,
					true);
			if (cr.isError()) {
				try {
					cr.throwException();
				} catch (CharacterCodingException ex) {
					throw new RuntimeException(ex);
				}
			}
			off = buffer.position();
			byte[] b = e.getValue();
			temp[off++] = b[0];
			temp[off++] = b[1];
			out.write(temp, 0, off);
			all += off;
		}
		CloseUtil.close(out);
		MathUtil.int2Byte(out.array(), all, 0);
		outputStream.write(out.array(), 0, all + 4);
		outputStream.flush();

	}

	private byte[] putCache(String key) {
		int hash = cacheHash++;
		cacheIntegerMap.put(Integer.valueOf(hash), key);
		byte[] b = new byte[2];
		MathUtil.short2Byte(b, (short) hash, 0);
		b[0] = (byte) (b[0] | CACHE_FLAG);
		cacheStringMap.put(key, b);
		return b;
	}

	public byte[] getCache(String key) {
		byte[] b = cacheStringMap.get(key);
		if (b == null) {
			if (key.length() <= 32 && canAdd()) {
				synchronized (cacheStringMap) {
					b = cacheStringMap.get(key);
					if (b != null) {
						return b;
					}
					if (!canAdd()) {
						return null;
					}
					return putCache(key);
				}
			}
			return null;
		}
		return b;
	}

	private boolean canAdd() {
		return cacheStringMap.size() < MAX_MAP_SZIE;
	}

	public static boolean isCache(byte b) {
		return b < 0;
	}

}
