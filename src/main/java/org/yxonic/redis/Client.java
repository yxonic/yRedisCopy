package org.yxonic.redis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Client {
    private static final Logger log = LogManager.getLogger();

    private static final int PROTO_REQ_INLINE = 1;
    private static final int PROTO_REQ_MULTIBULK = 2;
    private static final int PROTO_IOBUF_LEN = 16*1024;
    private static final int PROTO_REPLY_CHUNK_BYTES = 16*1024;
    private static final int PROTO_MBULK_BIG_ARG = 32*1024;
    private static final int CLIENT_CLOSED_AFTER_REPLY = 1 << 6;

    private static final boolean C_OK = true;
    private static final boolean C_ERR = false;

    // for each processing phase, reset after we successfully parsed a command
    private int reqType = 0;
    private int flags;
    private List<String> args;
    private int multibulkLength = 0;
    private int bulkLength = -1;

    // buffer for input/output
    private ByteBuffer inputBuffer = ByteBuffer.allocate(PROTO_IOBUF_LEN);
    private ByteBuffer replyBuffer = ByteBuffer.allocate(PROTO_REPLY_CHUNK_BYTES);

    // for input byte processing
    private int start;
    private int end;
    private byte[] input;

    // for k-v db demo
    private Map<String, String> db = new HashMap<>();

    void processInput() {
        input = inputBuffer.array();
        start = 0;
        end = inputBuffer.position();
        inputBuffer.clear();
        while (start < end) {
            if ((flags & CLIENT_CLOSED_AFTER_REPLY) != 0) {
                break;
            }

            if (reqType == 0) {
                if (input[start] == '*') {
                    start++;
                    reqType = PROTO_REQ_MULTIBULK;
                } else {
                    reqType = PROTO_REQ_INLINE;
                }
            }

            if (reqType == PROTO_REQ_INLINE) {
                if (processInlineBuffer() != C_OK)
                    break;
            } else if (reqType == PROTO_REQ_MULTIBULK) {
                if (processMultibulkBuffer() != C_OK)
                    break;
            }

            if (processCommand() == C_OK)
                reset();
        }
        inputBuffer.position(start);
        if (start < end)
            reallocateInput(end - start);
        else
            inputBuffer.clear();
    }

    ByteBuffer getInput() {
        return inputBuffer;
    }

    private void reallocateInput(int extraLength) {
        int readLen = PROTO_IOBUF_LEN;
        if (multibulkLength > 0 && bulkLength >= PROTO_MBULK_BIG_ARG)
            readLen = bulkLength + 2 - extraLength;
        ByteBuffer newInput = ByteBuffer.allocate(extraLength + readLen);
        newInput.put(inputBuffer);
        inputBuffer = newInput;
    }

    ByteBuffer getReply() {
        replyBuffer.flip();
        return replyBuffer;
    }

    private boolean processInlineBuffer() {
        int pos = Protocol.indexOf(input, start, end, (byte) '\n');
        if (pos == -1)
            return C_ERR;
        if (pos > start && input[pos-1] == '\r')
            pos--;
        args = Protocol.splitArgs(input, start, pos);
        if (args == null) {
            // TODO: add reply error, don't log
            return C_ERR;
        }
        if (args.size() == 0)
            return C_ERR;
        /* update position, skip \r\n */
        start = pos + 2;

        /* TODO: create redis object for each argument */

        return C_OK;
    }

    private boolean processMultibulkBuffer() {
        if (multibulkLength == 0) {
            int pos = Protocol.indexOf(input, start, end, (byte) '\r');
            if (pos == -1)
                return C_ERR;
            if (pos+1 >= end || input[pos+1] != '\n')
                return C_ERR;
            multibulkLength = Integer.parseInt(new String(input, start, pos - start));
            if (multibulkLength > 1024*1024) {
                // TODO: add reply error
                return C_ERR;
            }
            start = pos + 2;
            if (multibulkLength <= 0) {
                return C_OK;
            }
        }
        if (args == null)
            args = new ArrayList<>();
        while (multibulkLength > 0) {
            if (bulkLength == -1) {
                int pos = Protocol.indexOf(input, start, end, (byte) '\r');
                if (pos == -1)
                    break;
                if (pos + 1 >= end || input[pos + 1] != '\n')
                    break;
                if (input[start] != '$') {
                    // TODO: add reply error
                    return C_ERR;
                }
                start++;
                bulkLength = Integer.parseInt(new String(input, start, pos - start));
                if (bulkLength < 0 || bulkLength > 512 * 1024 * 1024) {
                    // TODO: add reply error
                    return C_ERR;
                }
                start = pos + 2;
                if (bulkLength >= PROTO_MBULK_BIG_ARG)
                    break;
            }
            if (start + bulkLength + 2 > end)
                break;
            if (start == 0 && bulkLength >= PROTO_MBULK_BIG_ARG) {
                assert (end == start + bulkLength + 2);
                // TODO: avoid copying
                args.add(new String(input).trim());
                start = end;
            } else {
                args.add(new String(input, start, bulkLength));
                start += bulkLength + 2;
            }
            bulkLength = -1;
            multibulkLength--;
        }
        if (multibulkLength == 0)
            return C_OK;
        return C_ERR;
    }

    private boolean processCommand() {
        if (args.size() == 0)
            return C_ERR;

        if (args.get(0).equalsIgnoreCase("quit")) {
            addReply("+OK\r\n");
            flags |= CLIENT_CLOSED_AFTER_REPLY;
            return C_ERR;
        }

        if (args.get(0).equalsIgnoreCase("get")) {
            if (args.size() < 2)
                return C_ERR;
            addReply(db.get(args.get(1)) + "\r\n");
        }
        else if (args.get(0).equalsIgnoreCase("set")) {
            if (args.size() < 2)
                return C_ERR;
            db.put(args.get(1), args.get(2));
            addReply("+OK\r\n");
        }
        return C_OK;
    }

    private void addReply(String s) {
        replyBuffer.put(s.getBytes());
    }

    private void reset() {
        reqType = 0;
        multibulkLength = 0;
        bulkLength = -1;
        args = null;
    }
}
