package com.alibaba.middleware.race.sync.log;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.model.RecordLog;

/**
 * Created by xiefan on 6/21/17.
 */
public class DeleteLog extends ChangeLog{

    public static int LEN = 5;

    public DeleteLog(RecordLog r){
        this.pk = r.getPrimaryColumn().getLongValue();
        this.alterType = Constants.DELETE;
    }
}
