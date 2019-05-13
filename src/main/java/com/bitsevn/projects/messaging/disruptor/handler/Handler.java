package com.bitsevn.projects.messaging.disruptor.handler;

import com.bitsevn.projects.messaging.disruptor.event.DataEvent;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Handler implements EventHandler<DataEvent<String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Handler.class);

    private String name;

    public Handler(String name) {
        this.name = name;
    }

    @Override
    public void onEvent(DataEvent<String> event, long sequence, boolean batchEnd) {
        LOGGER.debug("[handler-{}] handling event: {}", name, event);
    }
}
