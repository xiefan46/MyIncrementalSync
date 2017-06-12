package dbenchmark;

import com.alibaba.middleware.race.sync.model.Record;
import dbenchmark.db.Chronicle;
import dbenchmark.db.H2MVStore;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.h2.mvstore.MVMap;
import org.junit.Test;
import util.MockDataUtil;

import java.util.Random;

/**
 * Created by Rudy Steiner on 2017/6/11.
 */
public class ChronicleTest {

    @Test
    public void putTest(){
        Long key=10000000L;
        long kvNum=10000000;  //1000w
        long  mod=(long)(kvNum*0.1);
        ChronicleMap<String,String> map= Chronicle.getChronicMap("m3");
        //map.put()
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String k=Long.toString(key+i);
            String v= MockDataUtil.getRamdonString(30);
            map.put(k,v);
            if(i%mod==0){
                String getV=map.get(k);
                System.out.println("Chronic k:"+k+",v:"+v+",get v:"+getV);
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,put into Chronic :"+elapse+" ms");
        i=0;
        t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String val= map.get(Long.toString(key+i));
            if(i%mod==0){
                System.out.println("k:"+(key+i)+",v:"+val);
            }
        }
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,get from Chronic:"+elapse+" ms");
        t1=System.currentTimeMillis();

    }
    @Test
    public void putAndGetRecord(){
        Random random=new Random();
        Long key=10000000L;
        int kvNum=5000000;  //1000w
        long  mod=(long)(kvNum*0.1);
        ChronicleMap<Long,Record> map= Chronicle.getRecordMap("m3");
        //map.put()
        long i=0;
        int columns=3;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            Long k=key+i;
            Record v = new Record(columns);
            for(int j=0;j<columns;j++) {
                v.setColum(j,MockDataUtil.getRamdonString(random.nextInt(10)+1).getBytes());
            }
            map.put(k,v);
            if(i%mod==0){
                Record getV=map.get(k);
                System.out.println("MVStore k:"+(key+i)+",v:"+v.toString()+",get v:"+getV.toString());
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        //H2MVStore.close();
        System.out.println(kvNum+" kv, put into Chronicle :"+elapse+" ms");
        t1=System.currentTimeMillis();
        //H2MVStore.close();
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv, close Chronicle :"+elapse+" ms");
    }
    @Test
    public void getRecord(){
        Long key=10000000L;
        long kvNum=5000000; //100w
        long  mod=(long)(kvNum*0.1);
        ChronicleMap<Long,Record> map= Chronicle.getRecordMap("m3");
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            Long k=key+i;
            Record val= map.get(k);
            if(i%mod==0){
                System.out.println("k:"+(key+i)+",v:"+val);
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,get record from Chronicle:"+elapse+" ms");
    }

}
