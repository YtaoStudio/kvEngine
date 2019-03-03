package com.alibabacloud.polar_race.engine.util;

import gnu.trove.map.hash.TLongIntHashMap;
import org.junit.Test;

/**
 * @version V1.0
 * @Package: com.alibabacloud.polar_race.engine.util
 * @ClassName: TLongIntHashMapTest
 * @Description:
 * @author: tao
 * @date: Create in 2018-11-08 08:44
 **/
public class TLongIntHashMapTest {

    @Test
    public void testMap() {
        TLongIntHashMap keyOffMap = new TLongIntHashMap();
        keyOffMap.put(3, 4);
        keyOffMap.put(3, 5);
        System.out.println(keyOffMap.put(2, 6));
        System.out.println(keyOffMap.size());
        System.out.println(keyOffMap.get(3));
        System.out.println(keyOffMap.get(4));
    }
}
