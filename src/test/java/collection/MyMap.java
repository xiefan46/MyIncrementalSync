package collection;

import com.koloboke.compile.KolobokeMap;

import java.util.Map;

/**
 * Created by Rudy Steiner on 2017/6/22.
 */
@KolobokeMap
abstract class MyMap<K,V> implements Map<K,V> {
    static <K, V> Map<K, V> withExpectedSize(int expectedSize) {
        return new KolobokeMyMap<K, V>(expectedSize);
    }
}
