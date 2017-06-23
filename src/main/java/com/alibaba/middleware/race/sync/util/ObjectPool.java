package com.alibaba.middleware.race.sync.util;

import java.util.Arrays;

public class ObjectPool<V> {

	private VFactory<V>	vFactory;

	private V[]		vs;

	private int		capacity;

	private int		index	= -1;
	
	public ObjectPool(VFactory<V> factory) {
		this(2, factory);
	}

	public ObjectPool(int capacity,VFactory<V> factory) {
		if (capacity % 2 != 0) {
			throw new RuntimeException("capacity % 2 != 0");
		}
		this.capacity = capacity;
		this.vFactory = factory;
		this.vs = (V[]) new Object[capacity];
	}

	public V get() {
		if (index == -1) {
			return vFactory.newInstance();
		}
		return vs[index--];
	}

	public void put(V v) {
		vFactory.clean(v);
		if (++index == capacity) {
			resize();
		}
		vs[index] = v;
	}

	private void resize() {
		int oldCapacity = vs.length;
		int newCapacity = oldCapacity + (oldCapacity >> 1);
		capacity = newCapacity;
		vs = Arrays.copyOf(vs, newCapacity);
	}

	public static void main(String[] args) {
		
		ObjectPool<Integer> pool = new ObjectPool<>(new VFactory<Integer>() {
			@Override
			public Integer newInstance() {
				return 999;
			}
			@Override
			public void clean(Integer v) {
				
			}
		});
		
		int time = 10;
		
		for (int i = 0; i < time; i++) {
			int v = pool.get();
			pool.put(v);
		}
		
		
	}
	

}
