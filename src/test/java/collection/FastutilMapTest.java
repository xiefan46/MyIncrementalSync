package collection;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import javolution.util.FastMap;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.Test;
import util.MockDataUtil;
import vlsi.utils.CompactHashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Rudy Steiner on 2017/6/21.
 */
public class FastutilMapTest {
  //insert500W，D500W，U9000W
    final int w=10000;
    final int delete=500*w;
    final int D=0;
    final int insertScale=2;
    final int updateScale=18;
    final int insert=insertScale*delete;
    final int update=updateScale*delete;
    @Test
    public void fastUtilMapTest(){
        Random r=new Random();
        //fastUtil
        Int2ObjectMap<byte[][]> map=new Int2ObjectOpenHashMap(delete*insertScale);
        //hppc
       // IntObjectOpenHashMap<byte[][]> map=new IntObjectOpenHashMap<>(delete*insertScale);
        //gs map
        //IntObjectHashMap<byte[][]> map=new IntObjectHashMap<>(delete*insertScale);
        //kolokobe
        //Map<Integer,byte[][]> map = MyMap.withExpectedSize(delete*insertScale);
        //jdk hashMap
       // Map<Integer,byte[][]> map=new HashMap(delete*insertScale,1);
        // pauseless map
        //Map<Integer,byte[][]> map= new PauselessHashMap<>();
        // Koloboke int-2-object map test
        // fast map
        //FastMap<Integer,byte[][]> map=new FastMap<>();
        // chronic map
//        ChronicleMap map =
//                ChronicleMapBuilder.of(Integer.class, byte[][].class).
//                        entries(delete*insertScale*2)
//                        .name("map")
//                        .create();
        //smoothie map jdk 8
       // Map<Integer, byte[][]> map = new net.openhft.smoothie.SmoothieMap<>();
        // trove  map
        //compact map
       // CompactHashMap<Integer,  byte[][]> map = new CompactHashMap<>();
        //TIntObjectMap<byte[][]> map= new TIntObjectHashMap<>( delete*insertScale);
        long t1=System.currentTimeMillis();
       for(int i=0;i<delete;i++){
          for(int j=0;j<insertScale;j++){
              map.put(i*insertScale+j,mockByteArray(5,10));
          }
       }
       long t2=System.currentTimeMillis();
        System.out.println("insert "+insert+" data spend: "+(t2-t1)+" ms");
       for(int i=0;i<delete;i++){
           for(int j=0;j<updateScale;j++){
               map.put(r.nextInt(insert),mockByteArray(5,10));
           }
           map.remove(r.nextInt(insert));
       }
        long t3=System.currentTimeMillis();
        System.out.println("update "+update+" and delete "+delete+" in "+insert+" data spend: "+(t3-t2)+" ms");
        System.out.println("end");
    }

    public byte[][] mockByteArray(int rows,int columns){
        byte[][] mock=new byte[rows][columns];
        String data= MockDataUtil.getRamdonString(rows*columns);
        byte[] byteData=data.getBytes();
        for(int i=0;i<rows;i++){
           System.arraycopy(byteData,i*columns,mock[i],0,columns);
        }
        return mock;
    }
    //public byte[] mockBytes
    public void printMockData(byte[][] a){
       for(int i=a.length-1;i>=0;i--){
           System.out.println(Arrays.toString(a[i]));
        }
    }
    public byte[] mockByteArray(int len){

        return null;
    }
    @Test
    public void mockByte(){
        mockByteArray(1,5);
    }
}