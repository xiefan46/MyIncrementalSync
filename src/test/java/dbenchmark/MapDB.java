package dbenchmark;

import com.alibaba.middleware.race.sync.Constants;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.File;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rudy Steiner on 2017/6/10.
 */
public class MapDB {
    private static DB db= DBMaker.fileDB(new File(Constants.MAP_DB_FILE))
                         .asyncWriteEnable()
                         .asyncWriteFlushDelay(100)
                         .fileMmapEnable()
                         .make();
    private MapDB(){
        //TO DO
    }
    public static DB getInstance(){
        return db;
    }
    public  static ConcurrentMap<String,String> getConcurrentMap(String name){
        return db.hashMap(name,Serializer.STRING,Serializer.STRING);
    }
    public  void close(){
        db.close();
    }
    public void commit(){
        db.commit();
    }

}

