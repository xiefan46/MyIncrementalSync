package util;

import org.junit.Test;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by Rudy Steiner on 2017/7/8.
 */
public class ByteUtil {
    private final byte[] bytes=("0123456789abcdefghijklmnopqrstuvwxyz"
            + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").getBytes();
    private final int seedsLen=bytes.length;
    private Random rand=new Random();
    final int recordLen;
    byte[] result;
    public  ByteUtil(int len){
        recordLen=len;
        result =new byte[len];
    }
    public byte[] getRandomBytes(){
        for (int i = 0; i < recordLen; i++) {
            result[i] = bytes[rand.nextInt(seedsLen)];
        }
        return result;
    }
    @Test
   public void testRandomBytes(){
        int i=1000;
       while(i-->0){
          /// System.out.println(Arrays.toString(getRandomBytes(40)));
       }
   }

}
