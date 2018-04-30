package processor;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler {
    public Socket client;
    public PrintWriter printWriter;

    public ClientHandler(Socket client) {
        this.client = client;
        try {
            this.printWriter = new PrintWriter(client.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}