package com.alibaba.middleware.race.sync.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.alibaba.middleware.race.sync.Constants;

/**
 * Created by xiefan on 6/24/17.
 */
public class FileUtil {

    private static String getFilename(int index) {
        return String.format("%s/%010d.dat", Constants.MIDDLE_HOME, index);
    }

    public static String getString(long offset) {
        int index = (int) (offset >> 32);
        int off = (int) (offset & ((1<<32)-1));
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(getFilename(index), "r");
            randomAccessFile.seek(off);
            int len = randomAccessFile.readShort();
            byte[] buf = new byte[len];
            randomAccessFile.read(buf);
            String ret = new String(buf);
            System.out.println("len: " + len + ", text: " + ret);
            randomAccessFile.close();
            return ret;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String genFilename(int id, String suffix) {
        return String.format("%s/%010d.%s", Constants.MIDDLE_HOME, id, suffix);
    }
}
