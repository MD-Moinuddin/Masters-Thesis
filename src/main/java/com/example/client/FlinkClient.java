package com.example.client;

import com.example.tara.TaraConfig;
import com.example.tara.TaraLog;
import com.example.types.Payload;
import java.io.DataOutputStream;
import java.net.Socket;

public class FlinkClient {
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = TaraConfig.REQUEST_SOURCE_CLIENT_PORT;  // = 11100

        System.out.println("Connecting to " + host + ":" + port);

        try (Socket socket = new Socket(host, port);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            socket.setTcpNoDelay(true);
            System.out.println("Connected.");

            for (int xnr = 0; xnr < 10; xnr++) {
                byte[] data = ("test-payload-" + xnr).getBytes();
                Payload p = new Payload(0, xnr, data);
                Payload.writeDataStream(dos, p);
                dos.flush();
                System.out.println("Sent: " + p);
                Thread.sleep(200);
            }
            System.out.println("All sent. Waiting for execution...");
            Thread.sleep(3000);
        }
    }
}