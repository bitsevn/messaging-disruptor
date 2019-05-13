package com.bitsevn.projects.messaging.disruptor.publisher;

import com.bitsevn.projects.messaging.disruptor.event.DataEvent;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Publisher implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

    private String name;
    private long count;
    private Disruptor<DataEvent<String>> disruptor;

    public Publisher(String name, long count, Disruptor disruptor){
        this.name = name;
        this.count = count;
        this.disruptor = disruptor;
    }

    @Override
    public void run() {
        for(long c = 1; c <= count; c++) {
            final long stream = c;
            disruptor.publishEvent((event, sequence) -> {
                event.setPayload(String.format("%s%s", name, stream));
                // LOGGER.debug("[publisher-{}] published event: {}", name, event);
                System.out.println("[PUBLISHER-" + name + "]" + " publishing event " + event.getPayload());

            });
            try {
                TimeUnit.MICROSECONDS.sleep(ThreadLocalRandom.current().nextInt(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
