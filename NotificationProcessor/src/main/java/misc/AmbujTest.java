package misc;

import datamodel.WeatherNotificationData;
import processor.NotificationHandler;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AmbujTest {
    private static final int PORT = 9007;
    private static final List<Socket> socketList =
            Collections.synchronizedList(new ArrayList<Socket>());

    public static void main(String... args) {
        // create a thread to accept incoming client connections.
        Thread connectionThread = new Thread() {
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(PORT);
                    System.out.printf("\nJAVA server started \n");
                    while(true) {
                        Socket clientSocket = serverSocket.accept();
                        socketList.add(clientSocket);
                        System.out.println(clientSocket.getInetAddress().getHostName() + " connected");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };


        Thread consumerThread = new Thread() {
            public void run() {
                while (true) {
                    // do we need to set up poll timeout here?
                    List<WeatherNotificationData> weatherNotificationDataList =
                            new ArrayList<WeatherNotificationData>();

                    int counter = 1;

                    for(int i = 0; i < 5; i++) {
                        System.out.printf("\nGenerating weather events");
                        weatherNotificationDataList.add(
                                new WeatherNotificationData(
                                        44.97,
                                        93.26,
                                        11.0,
                                        21.0,
                                        1.0,
                                        String.format("%s%s", "Chilly Weather-", counter++)));
                    }
                    synchronized (socketList) {
                        for(Socket clientSocket : socketList) {
                            System.out.printf("\nCreating a new thread for each client to handle weather events");
                            NotificationHandler notificationHandler = new NotificationHandler(
                                    clientSocket, weatherNotificationDataList, 44.97, 93.26);
                            notificationHandler.run();
                        }
                    }
                }
            }
        };

        connectionThread.start();
        consumerThread.start();
    }
}
