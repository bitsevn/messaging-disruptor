package com.bitsevn.projects.messaging.disruptor.factory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DisruptorThreadFactory implements ThreadFactory {

    private AtomicInteger count = new AtomicInteger(0);
    private String name;

    public DisruptorThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name + "-" + count.incrementAndGet());
        t.setDaemon(true);
        return t;
    }
}
