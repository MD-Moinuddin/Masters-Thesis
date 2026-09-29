package com.example.client;

import com.example.tara.TaraConfig;
import com.example.types.Payload;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class TestClient {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = TaraConfig.REQUEST_SOURCE_CLIENT_PORT;

        System.out.println("Connecting to RequestSource at " + host + ":" + port);

        try (Socket s = new Socket(host, port);
             DataOutputStream dos = new DataOutputStream(s.getOutputStream());
             DataInputStream  dis = new DataInputStream(s.getInputStream())) {

            for (int cid = 1; cid <= 3; cid++) {
                Payload p = new Payload(cid, 0, ("hello-" + cid).getBytes());
                Payload.writeDataStream(dos, p);
                dos.flush();
                System.out.println("Sent request cid=" + cid);
            }

            for (int i = 0; i < 3; i++) {
                Payload reply = Payload.readDataStream(dis);
                if (reply == null) {
                    System.out.println("Connection closed before reply " + (i + 1));
                    break;
                }
                System.out.println("Reply cid=" + reply.cid
                        + " xnr=" + reply.xnr
                        + " data=" + new String(reply.any));
            }
        }

        System.out.println("Done.");
    }
}