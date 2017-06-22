package collection;


import org.junit.Test;

import java.util.Map;


/**
 * Created by Rudy Steiner on 2017/6/21.
 */
public class KolobokeMapTest {

    @Test
    public void mapTest(){

        Map<String, String> tickers = MyMap.withExpectedSize(10);
        tickers.put("AAPL", "Apple, Inc.");
        //Int2ByteArrayMap
        System.out.println("end");
    }
}
