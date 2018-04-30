package misc;

import datamodel.WeatherNotificationData;
import org.joda.time.DateTime;
import processor.ClientHandler;
import processor.NotificationHandler;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CarTest {
    private static final int PORT = 9007;
    private static final List<ClientHandler> socketList =
            Collections.synchronizedList(new ArrayList<ClientHandler>());

    public static void main(String... args) {
        // create a thread to accept incoming client connections.
        Thread connectionThread = new Thread() {
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(PORT);
                    System.out.printf("\nJAVA server started \n");
                    while(true) {
                        Socket clientSocket = serverSocket.accept();
                        socketList.add(new ClientHandler(clientSocket));
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
                        if(i < 3) {
                            weatherNotificationDataList.add(
                                    new WeatherNotificationData(
                                            "weather",
                                            "A",
                                            44.9723,
                                            -93.2625,
                                            67.0,
                                            11.0,
                                            1.0,
                                            String.format("%s%s", "Blizzard ", counter++)));
                        } else {
                            weatherNotificationDataList.add(
                                    new WeatherNotificationData(
                                            "weather",
                                            "B",
                                            44.9828,
                                            -93.1539,
                                            67.0,
                                            15.0,
                                            1.0,
                                            String.format("%s%s", "Normal Weather ", counter++)));
                        }
                    }
                    synchronized (socketList) {
                        for(ClientHandler clientSocket : socketList) {
                            System.out.printf("\nCreating a new thread for each client to handle weather events");
                            NotificationHandler notificationHandler = new NotificationHandler(
                                    clientSocket, weatherNotificationDataList, 44.97, 93.26);
                            notificationHandler.run();
                        }
                    }
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        connectionThread.start();
        consumerThread.start();
    }
}
