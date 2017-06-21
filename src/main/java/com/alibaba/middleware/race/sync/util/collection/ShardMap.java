package com.alibaba.middleware.race.sync.util.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Rudy Steiner on 2017/6/17.
 */
public class ShardMap<K,V> implements Map<K,V>{

    private List<Map> mapList;
    private int shards;
    private  final int mapCapacity;
    public ShardMap(int n,int capacity){
        shards=n;
        mapCapacity=capacity;
        mapList=new ArrayList<>();
        for(int i=0;i<shards;i++)
            mapList.add(new HashMap<>(mapCapacity));
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        Integer k=(Integer)key;
        int  slot=k%shards;
        if(slot>=0)
            return  (V)mapList.get(slot).get(key);
        else return null;
    }

    @Override
    public V put(K key, V value) {
        Integer k=(Integer)key;
        int  slot=k%shards;
        if(slot>=0)
            return  (V)mapList.get(slot).put(key,value);
        else return null;
    }

    @Override
    public V remove(Object key) {
        Integer k=(Integer)key;
        int  slot=k%shards;
        if(slot>=0)
            return  (V)mapList.get(slot).remove(key);
        return  null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
}
