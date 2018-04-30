package misc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import processor.ClientHandler;
import java.io.PrintWriter;
import java.util.List;

@AllArgsConstructor
public class CarNotificationHandler {
    private static PrintWriter printWriter;
    private static final double THRESHOLD = 1000.0;

    private ClientHandler clientSocket;
    private List<CarData> carDataList;
    private double latitude;
    private double longitude;

    public void run() {
        printWriter = clientSocket.printWriter;
        ObjectMapper mapper = new ObjectMapper();
        for(CarData carData : carDataList) {
                try {
                    String carNotification = mapper.writeValueAsString(carData);
                    // send notification
                    System.out.printf("\nSending car data to client: %s: %s",
                            clientSocket.client.getInetAddress().getHostName(),
                            carNotification);
                    printWriter.println(carNotification);
                    printWriter.flush();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
        }
    }
}
