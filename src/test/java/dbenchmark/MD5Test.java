package dbenchmark;

import com.alibaba.middleware.race.sync.util.FileUtils;
import com.generallycloud.baseio.common.FileUtil;
import org.junit.Test;

/**
 * Created by Rudy Steiner on 2017/6/13.
 */
public class MD5Test {

    @Test
    public void md5Test(){
        String path="/home/admin/test/";
        String name="RESULT.rs";
        String md5=FileUtils.MD5(path,name);
        System.out.println(md5);
    }
}
