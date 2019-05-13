package com.bitsevn.projects.messaging.disruptor.handler;

import com.bitsevn.projects.messaging.disruptor.event.DataEvent;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executors;

public class StreamDispatcherHandler implements EventHandler<DataEvent<String>> {

    private String name;
    private Disruptor<DataEvent<String>> downstreamAB;
    private Disruptor<DataEvent<String>> downstreamCD;
    private Disruptor<String> resultDownstream;

    public StreamDispatcherHandler(String name) {
        this.name = name;
        this.resultDownstream = initResultDownstreamDisruptor();

        this.downstreamAB = initDownstreamDisruptor("CONSUMER-AB", this.resultDownstream);
        this.downstreamCD = initDownstreamDisruptor("CONSUMER-CD", this.resultDownstream);

    }

    private Disruptor<String> initResultDownstreamDisruptor() {
        Disruptor<String> disruptor = new Disruptor<>(
                () -> "",
                1024,
                Executors.newFixedThreadPool(2),
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );
        disruptor.handleEventsWith(new ResultConsumer("RESULT-CONSUMER"));
        disruptor.start();
        return disruptor;
    }

    private Disruptor<DataEvent<String>> initDownstreamDisruptor(String consumerName, Disruptor<String> downstream) {
        Disruptor<DataEvent<String>> disruptor = new Disruptor<>(
                DataEvent.EVENT_FACTORY,
                1024,
                Executors.newFixedThreadPool(2),
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );
        disruptor.handleEventsWith(new EventConsumer(consumerName, downstream));
        disruptor.start();
        return disruptor;
    }

    @Override
    public void onEvent(DataEvent<String> event, long seq, boolean batchEbd) {
        if(event.getPayload().startsWith("A") || event.getPayload().startsWith("B")) {
            System.out.println("[" + name + "] dispatching event " + event.getPayload() + " to A/B downstream");
            publish(this.downstreamAB, event);
        } else if(event.getPayload().startsWith("C") || event.getPayload().startsWith("D")) {
            System.out.println("[" + name + "] dispatching event " + event.getPayload() + " to C/D downstream");
            publish(this.downstreamCD, event);
        }
    }

    private void publish(Disruptor<DataEvent<String>> target, DataEvent<String> sourceEvent) {
        target.publishEvent((event, seq) -> event.setPayload(sourceEvent.getPayload()));
    }
}
