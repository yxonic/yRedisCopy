package org.yxonic.redis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

public class RedisCLI {
    public static void main(String[] args) throws IOException {
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", 7345);
        SocketChannel client = SocketChannel.open(hostAddress);
        ByteBuffer inBuffer = ByteBuffer.allocate(1024);
        ByteBuffer outBuffer;

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            if (line.equals("exit"))
                break;
            outBuffer = ByteBuffer.wrap((line+"\r\n").getBytes());
            client.write(outBuffer);
            outBuffer.clear();

            int numRead = client.read(inBuffer);
            byte[] data = new byte[numRead];
            System.arraycopy(inBuffer.array(), 0, data, 0, numRead);
            System.out.println(new String(data));
            inBuffer.clear();
        }
    }
}
