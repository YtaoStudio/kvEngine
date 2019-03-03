package com.alibabacloud.polar_race.engine.cli;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultVisitor extends AbstractVisitor {

    public AtomicInteger visitCount = new AtomicInteger();
    private Map<KeyComparable, byte[]> kvMap;

    public DefaultVisitor(Map<KeyComparable, byte[]> kvMap) {
        this.kvMap = kvMap;
    }

    public void visit(byte[] key, byte[] value) {
        visitCount.incrementAndGet();
        if (this.kvMap != null) {
            byte[] oldValue = kvMap.get(new KeyComparable(key));
            if (!Arrays.equals(value, oldValue)) {
                try {
                    System.out.println("ERROR: value is not same\n" + Arrays.toString(value) + "\n" + Arrays.toString(oldValue));
                    System.exit(-1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
