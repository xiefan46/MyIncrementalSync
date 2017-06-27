package com.alibaba.middleware.race.sync;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 * Created by xiefan on 6/26/17.
 */
public class RangeSearcherTest {
    @Test
    public void test(){
        RangeSearcher searcher = new RangeSearcher(1,105,10);
        try{
            int id = searcher.searchForDealThread(0);
            fail("not correct");
        }catch (RuntimeException e){
            //correct
        }
        try{
            int id = searcher.searchForDealThread(105);
            fail("not correct");
        }catch (RuntimeException e){
            //correct
        }
        assertEquals(0,searcher.searchForDealThread(1));
        assertEquals(8,searcher.searchForDealThread(89));
        assertEquals(9,searcher.searchForDealThread(99));
        assertEquals(9,searcher.searchForDealThread(104));
    }
}
