package com.alibaba.middleware.race.sync.compress;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;

public final class UnsafeUtil {
	public static final Unsafe	UNSAFE;
	private static final Field	ADDRESS_ACCESSOR;

	private UnsafeUtil() {
	}

	static {
		try {
			Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			UNSAFE = (Unsafe) theUnsafe.get(null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		try {
			Field field = Buffer.class.getDeclaredField("address");
			field.setAccessible(true);
			ADDRESS_ACCESSOR = field;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static long getAddress(Buffer buffer) {
		try {
			return (long) ADDRESS_ACCESSOR.get(buffer);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
