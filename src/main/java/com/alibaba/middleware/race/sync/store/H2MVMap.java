package com.alibaba.middleware.race.sync.store;

import com.alibaba.middleware.race.sync.codec.RecordCodec;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.util.H2MVStore;
import org.h2.mvstore.MVMap;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by Rudy Steiner on 2017/6/12.
 */
public class H2MVMap<K, V> implements Map<K, V>{
    private H2MVStore mvStore;
    private MVMap<java.lang.Long,byte[]> map;
    private RecordCodec codec;
    public H2MVMap(H2MVStore store,String name){
        this.mvStore=store;
        this.map= mvStore.getByteMap(name);
        codec=new RecordCodec();
    }
    @Override
    public int size() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public V get(Object key) {
        byte[] bytes= map.get(key);
        if(bytes!=null&&bytes.length>0){
            return (V)codec.decode(bytes);
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        int offset=codec.encode((Record)value);
        byte[] bytes= new byte[offset];
        System.arraycopy(codec.getArray(),0,bytes,0,offset);
        bytes=map.put((Long)key,bytes);
        if(bytes!=null&&bytes.length>0){
           return (V)codec.decode(bytes);
        }
        return null;
    }

    @Override
    public V remove(Object key) {
        byte[] bytes= map.remove(key);
        if(bytes!=null&&bytes.length>0){
            return (V)codec.decode(bytes);
        }
        return null;
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> m) {
           throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @NotNull
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("unsupported method");
    }
}
