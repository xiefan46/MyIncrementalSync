/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.alibaba.middleware.race.sync.other.dictionary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.WeakHashMap;

/**
 */
public class ClassUtil {

    private static final Logger logger = LoggerFactory.getLogger(ClassUtil.class);

    public static void addClasspath(String path) {
        logger.info("Adding path " + path + " to class path");
        File file = new File(path);

        try {
            if (file.exists()) {
                URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                Class<URLClassLoader> urlClass = URLClassLoader.class;
                Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
                method.setAccessible(true);
                method.invoke(urlClassLoader, new Object[]{file.toURI().toURL()});
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final WeakHashMap<String, Class<?>> forNameCache = new WeakHashMap<>();

    @SuppressWarnings("unchecked")
    public static <T> Class<? extends T> forName(String name, Class<T> clz) throws ClassNotFoundException {
        String origName = name;

        Class<? extends T> result = (Class<? extends T>) forNameCache.get(origName);
        if (result == null) {
            result = (Class<? extends T>) Class.forName(name);
            forNameCache.put(origName, result);
        }
        return result;
    }


    public static Object newInstance(String clz) {
        try {
            return forName(clz, Object.class).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
