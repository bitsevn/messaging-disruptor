package com.bitsevn.projects.messaging.disruptor;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.Arrays;
import java.util.List;

public class DisruptorAppOld {

    public static void main(String[] args) {
        final List<String> GROUPS = Arrays.asList("A", "B", "C", "D");
        final int RING_BUFFER_SIZE = 64;
        final int STREAMS = 100;
        final int MAX_WORKERS = Runtime.getRuntime().availableProcessors();

        new DisruptorAppOld().run(GROUPS, RING_BUFFER_SIZE, STREAMS, MAX_WORKERS);

    }

    public void run(List<String> GROUPS, int RING_BUFFER_SIZE, int STREAMS, int MAX_WORKERS) {
        Disruptor<Event> disruptor1 = new Disruptor(
                Event.EVENT_FACTORY,
                RING_BUFFER_SIZE,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        Disruptor<Event> disruptorAB = new Disruptor(
                Event.EVENT_FACTORY,
                RING_BUFFER_SIZE,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        Disruptor<Event> disruptorCD = new Disruptor(
                Event.EVENT_FACTORY,
                RING_BUFFER_SIZE,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );


        EventHandler<Event> dispatcher = (e, s, b) -> {
            System.out.println("[dispatcher] event " + e);
            if(e.value.startsWith("A") || e.value.startsWith("B")) {
                disruptorAB.publishEvent((ev, sq) -> ev.value = e.value);
                // DISPATCHED_A.put(e.value);
            } else if(e.value.startsWith("C") || e.value.startsWith("D")) {
                disruptorCD.publishEvent((ev, sq) -> ev.value = e.value);
                // DISPATCHED_B.put(e.value);
            }
        };

        disruptor1.handleEventsWith(dispatcher);

        EventHandler<Event> joiner = (e, s, b) -> System.out.println("[joiner] event " + e);

        disruptorAB.handleEventsWithWorkerPool(getWorkHandlers("AB", MAX_WORKERS)).then(joiner);

        disruptorCD.handleEventsWithWorkerPool(getWorkHandlers("CD", MAX_WORKERS)).then(joiner);

        disruptor1.start();
        disruptorAB.start();
        disruptorCD.start();

        for(String group: GROUPS) {
            new Thread(() -> {
                for(int i=1; i<=STREAMS; i++) {
                    final int id = i;
                    disruptor1.publishEvent((e, s) -> e.value = group + id);
                }
                disruptor1.publishEvent((e, s) -> e.value = group + "!");
            }).start();
        }
    }

    private WorkHandler<Event>[] getWorkHandlers(String GROUP, int MAX_WORKERS) {
        WorkHandler<Event>[] workHandlers = new WorkHandler[MAX_WORKERS];

        for(int i=0; i<MAX_WORKERS; i++) {
            int workerId = i + 1;
            workHandlers[i] = (e) -> System.out.println("[worker-" + GROUP + "-" + workerId + "] event " + e);
        }
        return workHandlers;
    }

    static class Event {
        private String value;

        public Event() {}

        @Override
        public String toString() {
            return value;
        }

        public static final EventFactory<Event> EVENT_FACTORY = () -> new Event();
    }
}
