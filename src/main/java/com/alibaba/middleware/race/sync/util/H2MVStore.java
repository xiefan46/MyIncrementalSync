package com.alibaba.middleware.race.sync.util;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.Record;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/**
 * Created by Rudy Steiner on 2017/6/10.
 */
public class H2MVStore {
    private  MVStore mvStore;
    public H2MVStore(String name){
        mvStore= new MVStore.Builder()
                .fileName(Constants.H2_DB_FILE_HOME+"/"+name)
                .cacheSize(24) //default 16
//              .autoCommitDisabled()
//              .autoCommitBufferSize(24) //default 1024
//              .readOnly()
//              .compressHigh()
                .open();
    }
    public  MVMap<String,String> getMVMap(String name){
      return   mvStore.openMap(name);
    }
    public  MVMap<Long,Record>  getRecordMap(String name){
        return   mvStore.openMap(name);
    }
    public  MVMap<Long,byte[]> getByteMap(String name){
        return mvStore.openMap(name);
    }
    public  void close(){
        mvStore.close();
    }
}
