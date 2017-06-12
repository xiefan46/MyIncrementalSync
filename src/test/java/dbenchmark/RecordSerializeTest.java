package dbenchmark;

import com.alibaba.middleware.race.sync.codec.RecordCodec;
import com.alibaba.middleware.race.sync.model.Record;
import org.junit.Test;
import util.MockDataUtil;

import java.util.Random;

/**
 * Created by Rudy Steiner on 2017/6/12.
 */
public class RecordSerializeTest {

    @Test
    public void recordSerializeTest(){
        Random random=new Random();
        int columns=6;
        Record v = new Record(columns);
        for(int j=0;j<3;j++) {
            v.setColum(j*2, MockDataUtil.getRamdonString(random.nextInt(10)+1).getBytes());
        }
        RecordCodec codec=new RecordCodec();
                  int offset=codec.encode(v);
         byte[] bytes= new byte[offset];
         System.arraycopy(codec.getArray(),0,bytes,0,offset);
         Record newRecord=codec.decode(bytes);
         System.out.println("serialize end");

    }
}
