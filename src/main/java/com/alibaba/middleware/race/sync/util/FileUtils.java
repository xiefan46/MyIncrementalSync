package com.alibaba.middleware.race.sync.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Rudy Steiner on 2017/6/12.
 */
public class FileUtils {
    private Logger logger = LoggerFactory.getLogger(getClass());
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
    public static String MD5(String path,String name){
        long startTime = System.currentTimeMillis();
        String strMD5 = null;
        File file = new File(path+name);
        try {
            FileInputStream in = new FileInputStream(file);
            MappedByteBuffer buffer = in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0,
                    file.length());
            MessageDigest digest = MessageDigest.getInstance("md5");
            digest.update(buffer);
            in.close();
            byte[] byteArr = digest.digest();
            BigInteger bigInteger = new BigInteger(1, byteArr);
            strMD5 = bigInteger.toString(16);
            return  strMD5;
        }catch (IOException e){
            e.printStackTrace();
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        return null;
    }
}
