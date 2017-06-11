package dbenchmark.db;

import com.alibaba.middleware.race.sync.Constants;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/**
 * Created by Rudy Steiner on 2017/6/10.
 */
public class H2MVStore {

   // private static MVStore mvStore= MVStore.open(Constants.H2_DB_FILE);
    private static MVStore mvStore= new MVStore.Builder()
                                 .fileName(Constants.H2_DB_FILE)
                                 .cacheSize(24) //default 16
//                               .autoCommitDisabled()
//                               .autoCommitBufferSize(24) //default 1024
//                               .readOnly()
//                               .compressHigh()
                                 .open();
    public static MVMap<String,String> getMVMap(String name){
      return   mvStore.openMap(name);
    }
    public static void close(){
        mvStore.close();
    }
}
