package org.yxonic.redis;


import java.util.ArrayList;
import java.util.List;

public class Protocol {
    private static boolean isHexDigit(byte c) {
        if (c >= '0' && c <= '9')
            return true;
        if (c >= 'a' && c <= 'f')
            return true;
        if (c >= 'A' && c <= 'F')
            return true;
        return false;
    }

    private static byte hexDigitToByte(byte c) {
        if (c >= '0' && c <= '9')
            return (byte) (c - '0');
        if (c >= 'a' && c <= 'f')
            return (byte) (c - 'a' + 10);
        if (c >= 'A' && c <= 'F')
            return (byte) (c - 'A' + 10);
        return 0;
    }

    public static int indexOf(byte[] line, int start, int end, byte b) {
        int i = start;
        while (i < end) {
            if (line[i] == b)
                return i;
            i++;
        }
        return -1;
    }

    public static List<String> splitArgs(byte[] line, int start, int end) {
        ArrayList<String> result = new ArrayList<>();
        int i = start;
        outer:
        while (true) {
            while (line[i] == ' ' && i < end)
                i++;
            if (i == end) {
                return result;
            }
            boolean inQuotes = false;
            boolean inSingleQuotes = false;
            boolean done = false;
            StringBuilder current = new StringBuilder();
            while (!done) {
                if (inQuotes) {
                    if (i+3 < end &&
                            line[i] == '\\' &&
                            line[i+1] == 'x' &&
                            isHexDigit(line[i+2]) &&
                            isHexDigit(line[i+3]))
                    {
                        byte b = (byte) (hexDigitToByte(line[i+2])*16 + hexDigitToByte(line[i+3]));
                        current.append(b);
                        i += 3;
                    }
                    else if (line[i] == '\\' && i+1 < end) {
                        char c;
                        i++;
                        switch(line[i]) {
                            case 'n': c = '\n'; break;
                            case 'r': c = '\r'; break;
                            case 't': c = '\t'; break;
                            case 'b': c = '\b'; break;
                            case 'a': c = (char) 7; break;
                            default: c = (char) line[i]; break;
                        }
                        current.append(c);
                    } else if (line[i] == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (i+1 >= end || !Character.isWhitespace(line[i+1]))
                            break outer;
                        done = true;
                    } else if (i >= end) {
                        /* unterminated quotes */
                        break outer;
                    } else {
                        current.append((char) line[i]);
                    }
                }
                else if (inSingleQuotes) {
                    if (i+1 < end && line[i] == '\\' && line[i+1] == '\'') {
                        i++;
                        current.append('\'');
                    } else if (line[i] == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (i+1 >= end || !Character.isWhitespace(line[i+1]))
                            break outer;
                        done = true;
                    } else if (i >= end) {
                        /* unterminated quotes */
                        break outer;
                    } else {
                        current.append((char) line[i]);
                    }
                }
                else {
                    if (i >= end)
                        break;
                    switch (line[i]) {
                        case ' ':
                        case '\n':
                        case '\r':
                        case '\t':
                        case '\0':
                            done = true;
                            break;
                        case '"':
                            inQuotes = true;
                            break;
                        case '\'':
                            inSingleQuotes = true;
                            break;
                        default:
                            current.append((char) line[i]);
                            break;
                    }
                    i++;
                }
            }
            result.add(current.toString());
        }
        /* error occurred */
        return null;
    }
}
