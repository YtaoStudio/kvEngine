package com.alibabacloud.polar_race.engine.cli;


public class KeyComparable implements Comparable {
    byte[] key;

    public KeyComparable(byte[] key) {
        this.key = key.clone();
    }

    public static final KeyComparable ZERO = new KeyComparable(
            new byte[]{(byte) 0, (byte) 0, (byte) 0, (byte) 0,
                    (byte) 0, (byte) 0, (byte) 0, (byte) 0});

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    // byte 64位, 63 == 0x1F
    public static int getHashCode(byte[] key) {
        return (key[7] & 0xFF) % 64;
        // return 0;
    }

    @Override
    public int compareTo(Object o) {
        KeyComparable that = (KeyComparable) o;
        short thisKeyByte = 0, thatKeyByte = 0;
        for (int i = 0; i < 8; i++) {
            thisKeyByte = (short) (this.key[i] & 0xFF);
            thatKeyByte = (short) (that.key[i] & 0xFF);
            if (thisKeyByte != thatKeyByte) {
                return thisKeyByte - thatKeyByte;
            }
        }
        return 0;
    }

    public static void main(String args[]) {
        ;
    }
}
