package dbenchmark;
import dbenchmark.db.H2MVStore;
import dbenchmark.db.MapDB;
import org.junit.Test;

import util.MockDataUtil;

import java.util.concurrent.ConcurrentMap;


/**
 * Created by Rudy Steiner on 2017/6/10.
 */
public class MapDBTest {

    @Test
    public void  putAndGetTest(){
        Long key=10000000L;
        long kvNum=5000000;  //1000w
        long  mod=(long)(kvNum*0.1);
        ConcurrentMap<String,String>  map= MapDB.getMemoryConcurrentMap("m1");
        //map.put()
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String k=Long.toString(key+i);
            String v=MockDataUtil.getRamdonString(30);
            map.put(k,v);
            if(i%mod==0){
                String getV=map.get(k);
                System.out.println("map db k:"+(key+i)+",v:"+v+",get v:"+getV);
            }
        }

        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,put into map db:"+elapse+" ms");
        //map get()
        //map= MapDB.getMeConcurrentMap("m1");
        i=0;
        t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String val= map.get(Long.toString(key+i));
            if(i%mod==0){
                System.out.println("k:"+(key+i)+",v:"+val);
            }
        }
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,get from map db:"+elapse+" ms");
        t1=System.currentTimeMillis();
        //MapDB.getInstance().commit();
        H2MVStore.close();
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,commit to disk base map db:"+elapse+" ms");
    }
    // should
    //
    @Test
    public void getTest(){
        Long key=10000000L;
        long kvNum=5000000;  //100w
        long  mod=(long)(kvNum*0.1);
        ConcurrentMap<String,String>  map= MapDB.getConcurrentMap("m1");
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
         String val= map.get(Long.toString(key+i));
            if(i%mod==0){
                System.out.println("map db k:"+(key+i)+",v:"+val);
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,get from map db:"+elapse+" ms");
    }

}
