package dbenchmark;

import com.alibaba.middleware.race.sync.codec.RecordCodec;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.util.H2MVStore;
import org.h2.mvstore.MVMap;
import org.junit.Test;
import util.MockDataUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Rudy Steiner on 2017/6/12.
 */
public class RecordMapTest {
    /*
     * compare JDK HashMap with MVStore Map
     * 5000w Record
     * Record 4 column, per column max size 15 byte
     *  result shows that MVStore is faster 4 seconds than HashMap  on this condition
     * */
    @Test
    public void putAndGetRecord(){
        Random random=new Random();
        RecordCodec codec=new RecordCodec();
        Long key=10000000L;
        int kvNum=5000000;  //1000w
        long  mod=(long)(kvNum*0.1);
//      H2MVStore mvStore=new H2MVStore("h2_map.db");
//      MVMap<Long,byte[]> map= mvStore.getByteMap("m3");
        Map<Long,byte[]> map=new HashMap<>();
        //map.put()
        long i=0;
        int columns=4;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            Long k=key+i;
            Record v = new Record(columns);
            for(int j=0;j<columns;j++) {
                v.setColum(j, MockDataUtil.getRamdonString(random.nextInt(15)+1).getBytes());
            }
            int offset=codec.encode(v);
            byte[] bytes= new byte[offset];
            System.arraycopy(codec.getArray(),0,bytes,0,offset);
            map.put(k,bytes);
            if(i%mod==0){
                byte[] mapV=map.get(k);
                Record mapRecord=codec.decode(mapV);
                System.out.println("MVStore k:"+(key+i)+",v:"+v.toString()+",get v:"+mapRecord.toString());
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv, put into MVStore :"+elapse+" ms");
        t1=System.currentTimeMillis();
        //mvStore.close();
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv, close MVStore :"+elapse+" ms");
    }
    @Test
    public void getRecord(){
        RecordCodec codec=new RecordCodec();
        Long key=10000000L;
        long kvNum=5000000; //100w
        long  mod=(long)(kvNum*0.1);
        H2MVStore mvStore=new H2MVStore("h2_map.db");
        MVMap<Long,byte[]> map= mvStore.getByteMap("m3");
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            Long k=key+i;
            byte[] mapV=map.get(k);
            Record mapRecord=codec.decode(mapV);
            if(i%mod==0){
                System.out.println("k:"+(key+i)+",v:"+mapRecord);
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,get record from MVStore:"+elapse+" ms");
    }



}
