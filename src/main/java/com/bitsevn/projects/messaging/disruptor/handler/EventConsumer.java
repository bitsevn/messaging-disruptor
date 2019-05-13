package com.bitsevn.projects.messaging.disruptor.handler;

import com.bitsevn.projects.messaging.disruptor.event.DataEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.*;

public class EventConsumer implements EventHandler<DataEvent<String>> {

    private String name;
    private Disruptor<String> downstream;
    private ExecutorService executorService;

    public EventConsumer(String name, Disruptor<String> downstream) {
        this.name = name;
        this.downstream = downstream;
        this.executorService = Executors.newFixedThreadPool(5);
    }

    @Override
    public void onEvent(DataEvent<String> event, long seq, boolean batchEnd) {
        System.out.println("[" + name + "] consuming event " + event.getPayload());
        Future<String> future = executorService.submit(() -> {
            int count = ThreadLocalRandom.current().nextInt(50);
            while (count > 0) count--;// busy spin
            return ":processed";
        });
        this.downstream.publishEvent((_event, _seq) -> {
            try {
                _event.concat(future.get());
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
