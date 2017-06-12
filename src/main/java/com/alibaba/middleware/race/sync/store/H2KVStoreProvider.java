package com.alibaba.middleware.race.sync.store;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.Record;
import com.alibaba.middleware.race.sync.util.FileUtils;
import com.alibaba.middleware.race.sync.util.H2MVStore;

import java.util.Map;

/**
 * Created by xiefan on 6/12/17.
 */
public class H2KVStoreProvider implements KVStoreProvider{
    private final String defaultDBName="h2_map.db";
    private final String defaultMapName="map_1.db";
    private String dbName=defaultDBName;
    private  String mapName=defaultMapName;
    private H2MVStore store;
    public H2KVStoreProvider(boolean clear){
        if(clear)
            clear();
        this.store=new H2MVStore(dbName);
    }
    public H2KVStoreProvider(String dbName,String mapName,boolean clear){
        this.dbName=dbName;
        this.mapName=mapName;
        if(clear)
            clear();
        this.store=new H2MVStore(this.dbName);
    }
    @Override
    public Map<Long, Record> provide() {
        //包装h2代码
        return new H2MVMap<Long,Record>(store,mapName);
    }
    private boolean clear(){
        boolean del= FileUtils.delete(Constants.H2_DB_FILE_HOME,"/"+this.dbName);
        if(del==true){
            System.out.println("ok,clear");
        }else{
            System.out.println("file not found");
        }
        return  del;
    }

}
