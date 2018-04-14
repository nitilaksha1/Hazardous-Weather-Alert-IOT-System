package app;

import consumer.StreamDataConsumer;

public class StreamApp {
    public static void main(String... args) {
        StreamDataConsumer streamDataConsumer = new StreamDataConsumer();
        streamDataConsumer.consume();
    }
}
