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

public class NayanTest {
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

        Thread carThread = new Thread() {
            public void run() {
                while (true) {
                    // do we need to set up poll timeout here?
                    List<CarData> carDataList = new ArrayList<CarData>();

                    int counter = 1;
                    String[] latArr = new String[]
                            {"44.930607", "44.923924", "44.934099", "44.942482", "44.9433",
                                    "44.947935", "44.963715", "44.965505", "44.977936"};

                    String[] longArr = new String[]
                            {"-93.348760", "-93.375711", "-93.337774", "-93.326616", "-93.3212",
                                    "-93.301365", "-93.290223", "-93.266233", "-93.256577"};

                    for(int i = 0; i < 9; i++) {
                        System.out.printf("\nGenerating car movement");
                        carDataList.add(
                                new CarData(
                                        "car",
                                        i + "",
                                        latArr[i],
                                        longArr[i],
                                        new DateTime().getMillis(),
                                        45,
                                        20));
                    }
                    synchronized (socketList) {
                        for(ClientHandler clientSocket : socketList) {
                            System.out.printf("\nCreating a new thread for each client to handle weather events");
                            CarNotificationHandler carNotificationHandler = new CarNotificationHandler(
                                    clientSocket, carDataList, 44.97, 93.26);
                            carNotificationHandler.run();
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
        carThread.start();
    }
}

