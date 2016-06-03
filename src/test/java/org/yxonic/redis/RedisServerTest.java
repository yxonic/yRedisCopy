package org.yxonic.redis;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RedisServerTest {
    @Test
    public void testRedisServer() throws IOException, InterruptedException {
        RedisServer server = new RedisServer();
        Thread thread = new Thread(() -> {
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Should not have thrown any exception");
            }
        });

        thread.setDaemon(true);
        thread.start();

        Thread.sleep(500);

        InetSocketAddress hostAddress = new InetSocketAddress("127.0.0.1", 7345);
        SocketChannel client = SocketChannel.open(hostAddress);

        byte[] message = "set hello world\r\n".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(message);
        client.write(buffer);
        buffer = ByteBuffer.allocate(1024);
        int numRead = client.read(buffer);
        assertEquals("+OK\r\n", new String(buffer.array(), 0, numRead));

        message = "set world alpaca\r\n".getBytes();
        buffer = ByteBuffer.wrap(message);
        client.write(buffer);
        buffer = ByteBuffer.allocate(1024);
        numRead = client.read(buffer);
        assertEquals("+OK\r\n", new String(buffer.array(), 0, numRead));

        message = "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n".getBytes();
        buffer = ByteBuffer.wrap(message);
        client.write(buffer);
        buffer = ByteBuffer.allocate(1024);
        numRead = client.read(buffer);
        assertEquals("world\r\n", new String(buffer.array(), 0, numRead));

        message = "*2\r\n$3\r\nget\r\n$5\r\nworld\r\n".getBytes();
        buffer = ByteBuffer.wrap(message);
        client.write(buffer);
        buffer = ByteBuffer.allocate(1024);
        numRead = client.read(buffer);
        assertEquals("alpaca\r\n", new String(buffer.array(), 0, numRead));
    }
}