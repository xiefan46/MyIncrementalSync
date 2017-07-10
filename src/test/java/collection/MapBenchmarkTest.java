package collection;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.junit.Test;
import util.ByteUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Rudy Steiner on 2017/7/8.
 */
public class MapBenchmarkTest {
    final int w=10000;
    final int delete=500*w;
    final int D=0;
    final int insertScale=2;
    final int updateScale=18;
    final int insert=insertScale*delete;
    final int update=updateScale*delete;
    final int recordLen=40;
    final ByteUtil byteUtil=new ByteUtil(recordLen);
    @Test
    public void HPPCTest(){
        Random r=new Random();
        //fastutil
       // Int2ObjectMap<byte[]> map=new Int2ObjectOpenHashMap(delete*insertScale);
        //jdk
       // Map<Integer,byte[]> map=new HashMap(delete*insertScale);
        //hppc
        IntObjectOpenHashMap<byte[]> map=new IntObjectOpenHashMap<>(delete*insertScale);
        long t1=System.currentTimeMillis();
        for(int i=0;i<delete;i++){
            for(int j=0;j<insertScale;j++){
                byte[] bytes=new byte[recordLen];
                System.arraycopy(byteUtil.getRandomBytes(),0,bytes,0,recordLen);
//                if(i%10000==0)
//                    System.out.println(Arrays.toString(bytes));
                map.put(i*insertScale+j,bytes);
            }
        }
        long t2=System.currentTimeMillis();
        System.out.println("insert "+insert+" data spend: "+(t2-t1)+" ms");
        for(int i=0;i<delete;i++){
            for(int j=0;j<updateScale;j++){
                byte[] bytes=map.get(r.nextInt(insert));
                if(bytes!=null){
                    //update
                    System.arraycopy(byteUtil.getRandomBytes(),0,bytes,0,recordLen);
                }
            }
            map.remove(r.nextInt(insert));
        }
        long t3=System.currentTimeMillis();
        System.out.println("update "+update+" and delete "+delete+" in "+insert+" data spend: "+(t3-t2)+" ms");
        System.out.println("end");

    }
}
