package com.bitsevn.projects.messaging.disruptor;

import java.util.Arrays;

public class DisruptorApplication {

    public static void main(String[] args) {
        DisruptorServer disruptorServer = new DisruptorServer();
        disruptorServer.setDebugEnabled(true);
        disruptorServer.start(1024, 5, 4, Arrays.asList("A", "B", "C", "D"));
    }
}
