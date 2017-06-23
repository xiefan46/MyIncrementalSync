package com.alibaba.middleware.race.sync.util;

public interface VFactory<V> {

	V newInstance();
	
	void clean(V v);
}
