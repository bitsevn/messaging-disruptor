package com.bitsevn.projects.messaging.disruptor.handler;

import com.lmax.disruptor.EventHandler;

public class ResultConsumer implements EventHandler<String> {

    private String name;

    public ResultConsumer(String name) {
        this.name = name;
    }

    @Override
    public void onEvent(String event, long seq, boolean batchEnd) {
        System.out.println("[" + name + "] result event" + event);
    }
}
