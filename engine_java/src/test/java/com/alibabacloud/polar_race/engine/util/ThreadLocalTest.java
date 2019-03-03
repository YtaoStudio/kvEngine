package com.alibabacloud.polar_race.engine.util;

import org.junit.Test;

/**
 * @version V1.0
 * @Package: com.alibabacloud.polar_race.engine.util
 * @ClassName: ThreadLocalTest
 * @Description:
 * @author: tao
 * @date: Create in 2018-11-08 15:36
 **/
public class ThreadLocalTest {

    private ThreadLocal<Integer> vLog = new ThreadLocal<>();

    @Test
    public void testThread() {
        System.out.println(vLog.get());

        int no = -1;
        if (vLog.get() != null)
            no = vLog.get();
        System.out.println(no);

        vLog.set(10);
        no = vLog.get();
        System.out.println(no);
    }
}
