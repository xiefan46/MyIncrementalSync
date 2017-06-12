package com.alibaba.middleware.race.sync.util;

import java.io.File;

/**
 * Created by Rudy Steiner on 2017/6/12.
 */
public class FileUtils {


    /*
   * @return false if not exist or delete fail
   * */
    public static boolean delete(String path, String name) {
        File file = new File(path + name);
        if (file.exists()) {
            return file.delete();
        }
        return false;
    }
}
