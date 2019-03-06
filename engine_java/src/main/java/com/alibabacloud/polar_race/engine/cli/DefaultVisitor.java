package com.alibabacloud.polar_race.engine.cli;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultVisitor extends AbstractVisitor {

    public AtomicInteger visitCount = new AtomicInteger();
    private Map<KeyComparable, byte[]> kvMap;
    private KeyComparable lastKey;

    public DefaultVisitor(Map<KeyComparable, byte[]> kvMap) {
        this.kvMap = kvMap;
    }

    public void visit(byte[] key, byte[] value) {
        visitCount.incrementAndGet();
        if (this.kvMap != null) {
            KeyComparable newKey = new KeyComparable(key);
            byte[] oldValue = kvMap.get(newKey);
            if (!Arrays.equals(value, oldValue)) {
                System.out.println("ERROR: value is not same\n" + Arrays.toString(value) + "\n" + Arrays.toString(oldValue));
                System.exit(-1);
            }
            if (!checkSequence(newKey)) {
                System.out.println("ERROR: key is in order\n" + lastKey.toString());
                System.exit(-1);
            }
        }
    }

    private boolean checkSequence(KeyComparable newKey) {
        if (lastKey == null) {
            lastKey = newKey;
            return true;
        } else {
            return lastKey.compareTo(newKey) <= 0;
        }
    }

}
