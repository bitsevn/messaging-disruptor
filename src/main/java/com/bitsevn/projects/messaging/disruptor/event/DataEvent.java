package com.bitsevn.projects.messaging.disruptor.event;

import com.lmax.disruptor.EventFactory;

public class DataEvent<T> {

    private T payload;

    public DataEvent() {}

    public DataEvent(T payload) {
        this.payload = payload;
    }

    public static final EventFactory EVENT_FACTORY = () -> new DataEvent<String>();

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return String.format("DataEvent[payload = %s]", payload);
    }
}
