package org.yxonic.redis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RedisServer {
    private static final Logger log = LogManager.getLogger();

    private String bindAddress;
    private int port;
    private boolean stop = false;
    private Selector selector;
    private final Map<SelectionKey, Client> clients = new HashMap<>();
    private Client currentClient;

    public RedisServer() {
        bindAddress = "0.0.0.0";
        port = 7345;
    }

    public void start() throws IOException {
        selector = Selector.open();
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(bindAddress, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (!stop) {
            selector.select();
            Iterator keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = (SelectionKey) keys.next();
                keys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    accept(key);
                }
                else if (key.isReadable()) {
                    read(key);
                }
                else if (key.isWritable()) {
                    write(key);
                }
            }
        }
    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();
        channel.configureBlocking(false);
        SelectionKey channelKey = channel.register(selector, SelectionKey.OP_READ);
        clients.put(channelKey, new Client());
        log.info("New connection from: " + channel.getRemoteAddress().toString().trim());
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        currentClient = clients.get(key);
        ByteBuffer buffer = currentClient.getInput();
        int numRead = channel.read(buffer);
        if (numRead == -1) {
            channel.close();
            key.cancel();
            clients.remove(key);
            log.info("Connection closed by client");
            return;
        }
        log.debug("Received {} bytes", numRead);
        currentClient.processInput();
        key.interestOps(SelectionKey.OP_WRITE);
    }

    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        currentClient = clients.get(key);
        ByteBuffer buffer = currentClient.getReply();
        log.debug("Sent {} bytes", buffer.remaining());
        channel.write(buffer);
        buffer.clear();
        key.interestOps(SelectionKey.OP_READ);
    }

    public static void main(String[] args) {
        RedisServer server = new RedisServer();
        try {
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
