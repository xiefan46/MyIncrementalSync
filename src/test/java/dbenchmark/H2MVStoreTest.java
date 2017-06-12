package dbenchmark;

import com.alibaba.middleware.race.sync.codec.RecordCodec;
import com.alibaba.middleware.race.sync.model.Record;
import dbenchmark.db.H2MVStore;
import org.h2.mvstore.MVMap;
import org.junit.Test;
import util.MockDataUtil;

import java.util.Random;

/**
 * Created by Rudy Steiner on 2017/6/10.
 */
public class H2MVStoreTest {

    @Test
    public void  putAndGetTest(){
        Long key=10000000L;
        long kvNum=5000000;  //1000w
        long  mod=(long)(kvNum*0.1);
        MVMap<String,String> map= H2MVStore.getMVMap("m2");
        //map.put()
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String k=Long.toString(key+i);
            String v= MockDataUtil.getRamdonString(30);
            map.put(k,v);
            if(i%mod==0){
                String getV=map.get(k);
                System.out.println("MVStore k:"+k+",v:"+v+",get v:"+getV);
            }
        }

        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,put into MVStore :"+elapse+" ms");
        //map get()
        i=0;
        t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String val= map.get(Long.toString(key+i));
            if(i%mod==0){
                System.out.println("k:"+(key+i)+",v:"+val);
            }
        }
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,get from MVStore:"+elapse+" ms");
        t1=System.currentTimeMillis();
        //MapDB.getInstance().commit();
        //H2MVStore.close();
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,commit to disk base MVStore:"+elapse+" ms");
    }
    // should
    //
    @Test
    public void getTest(){
        Long key=10000000L;
        long kvNum=5000000; //100w
        long  mod=(long)(kvNum*0.1);
        MVMap<String,String> map= H2MVStore.getMVMap("m2");
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String val= map.get(Long.toString(key+i));
            if(i%mod==0){
                System.out.println("k:"+(key+i)+",v:"+val);
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,get from MVStore:"+elapse+" ms");
    }
    @Test
    public void randomGetTest(){
        Random random=new Random();
        int key=10000000;
        int kvNum=5000001; //100w
        long  mod=(long)(kvNum*0.1);
        MVMap<String,String> map= H2MVStore.getMVMap("m2");
        int i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            int u=random.nextInt(kvNum);
            String val= map.get(Integer.toString(key+u));
            if(i%mod==0){
                System.out.println("k:"+(key+u)+",v:"+val);
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,random get from MVStore:"+elapse+" ms");
    }
    @Test
    public void randomPutTest(){
        Random random=new Random();
        Long key=10000000L;
        int kvNum=5000000;  //1000w
        long  mod=(long)(kvNum*0.1);
        MVMap<String,String> map= H2MVStore.getMVMap("m2");
        //map.put()
        long i=0;
        long t1=System.currentTimeMillis();
        while(i++<=kvNum){
            String k=Long.toString(key+random.nextInt(kvNum));
            String v= MockDataUtil.getRamdonString(30);
            map.put(k,v);
            if(i%mod==0){
                String getV=map.get(k);
                System.out.println("MVStore k:"+(key+i)+",v:"+v+",get v:"+getV);
            }
        }
        long elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv,random put into MVStore :"+elapse+" ms");

    }

    @Test
    public void putAndGetRecord(){
        Random random=new Random();
        RecordCodec codec=new RecordCodec();
        Long key=10000000L;
        int kvNum=5000000;  //1000w
        long  mod=(long)(kvNum*0.1);
        //MVMap<String,Record> map= H2MVStore.getRecordMap("m3");
        MVMap<Long,byte[]> map= H2MVStore.getByteMap("m3");
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
        //H2MVStore.close();
        System.out.println(kvNum+" kv, put into MVStore :"+elapse+" ms");
        t1=System.currentTimeMillis();
        H2MVStore.close();
        elapse=System.currentTimeMillis()-t1;
        System.out.println(kvNum+" kv, close MVStore :"+elapse+" ms");
    }

    @Test
    public void getRecord(){
        RecordCodec codec=new RecordCodec();
        Long key=10000000L;
        long kvNum=5000000; //100w
        long  mod=(long)(kvNum*0.1);
        //MVMap<String,Record> map= H2MVStore.getRecordMap("m3");
        MVMap<Long,byte[]> map= H2MVStore.getByteMap("m3");
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
