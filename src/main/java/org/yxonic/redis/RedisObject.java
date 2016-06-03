package org.yxonic.redis;

import java.io.Serializable;
import java.nio.ByteBuffer;

class RedisObject implements Serializable {
    private static final byte OBJ_STRING = 0;
    private static final byte OBJ_LIST = 1;
    private static final byte OBJ_SET = 2;
    private static final byte OBJ_ZSET = 3;
    private static final byte OBJ_HASH = 4;

    private static final byte OBJ_ENCODING_RAW = 0;
    private static final byte OBJ_ENCODING_INT = 1;
    private static final byte OBJ_ENCODING_HT = 2;
    private static final byte OBJ_ENCODING_ZIPMAP = 3;
    private static final byte OBJ_ENCODING_LINKEDLIST = 4;
    private static final byte OBJ_ENCODING_ZIPLIST = 5;
    private static final byte OBJ_ENCODING_INTSET = 6;
    private static final byte OBJ_ENCODING_SKIPLIST = 7;
    private static final byte OBJ_ENCODING_EMBSTR = 8;
    private static final byte OBJ_ENCODING_QUICKLIST = 9;
}
