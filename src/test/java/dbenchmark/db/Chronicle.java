package dbenchmark.db;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.Record;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;

/**
 * Created by Rudy Steiner on 2017/6/11.
 */
public class Chronicle {

    public static ChronicleMap<String,String> getChronicMap(String name){
        try {
            ChronicleMap<String, String> chronicMap =
                    ChronicleMapBuilder.of(String.class, String.class)
                            .name(name)
                            .averageKeySize(8)
                            .averageValueSize(30)
                            .entries(1000_0000).createPersistedTo(new File(Constants.CHRONICLE_MAP_DB_FILE));
            return  chronicMap;
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }
    public static ChronicleMap<Long,Record> getRecordMap(String name){
        try {
            ChronicleMap<Long, Record> chronicMap =
                    ChronicleMapBuilder.of(Long.class, Record.class)
                            .name(name)
                           // .averageKeySize(8)
                            .averageValueSize(300)
                            .entries(500_0000).createPersistedTo(new File(Constants.CHRONICLE_MAP_DB_FILE));
            return  chronicMap;
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }
}
