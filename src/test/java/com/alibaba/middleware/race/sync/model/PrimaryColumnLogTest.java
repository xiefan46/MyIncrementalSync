package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.other.bytes.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by xiefan on 6/10/17.
 */
public class PrimaryColumnLogTest {

	@Test
	public void test() throws Exception {
		PrimaryColumnLog c = new PrimaryColumnLog();
		long one = 1L;
		byte[] longByte = Bytes.toBytes(1L);
		assertEquals(1L, Bytes.toLong(longByte));
		c.setLongValue(1);
		c.setBeforeValue(1);
		c.setValue(longByte, 0, longByte.length);
		assertTrue(!c.isPkChange());
		System.out.println(c.getValue() == one);

	}
}
