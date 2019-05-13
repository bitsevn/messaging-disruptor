package com.bitsevn.projects.messaging.disruptor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DisruptorServerTest {

    @Test
    public void test_ring_size_64_events_per_producer_10_workers_no_of_cores() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 64;
        final int eventsPerProducer = 10;
        final int workers = Runtime.getRuntime().availableProcessors();
        DisruptorServer disruptorServer = createDisruptorServer(producers, eventsPerProducer);
        disruptorServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_128_events_per_producer_100_workers_no_of_cores() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 128;
        final int eventsPerProducer = 100;
        final int workers = Runtime.getRuntime().availableProcessors();
        DisruptorServer disruptorServer = createDisruptorServer(producers, eventsPerProducer);
        disruptorServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_1024_events_per_producer_1000_workers_8() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 1024;
        final int eventsPerProducer = 1000;
        final int workers = 8;
        DisruptorServer disruptorServer = createDisruptorServer(producers, eventsPerProducer);
        disruptorServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_4096_events_per_producer_5000_workers_8() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 4096;
        final int eventsPerProducer = 5000;
        final int workers = 8;
        DisruptorServer disruptorServer = createDisruptorServer(producers, eventsPerProducer);
        disruptorServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    @Test
    public void test_ring_size_32768_events_per_producer_10000_workers_16() {
        final List<String> producers = Arrays.asList("A", "B", "C", "D");
        final int ringSize = 32768;
        final int eventsPerProducer = 10000;
        final int workers = 16;
        DisruptorServer disruptorServer = createDisruptorServer(producers, eventsPerProducer);
        disruptorServer.start(ringSize, eventsPerProducer, workers, producers);
    }

    private DisruptorServer createDisruptorServer(List<String> producers, int eventsPerProducer) {
        final int totalEvents = eventsPerProducer * producers.size();

        BlockingQueue<DisruptorServer.Event> productionQ = new ArrayBlockingQueue<>(totalEvents);
        BlockingQueue<DisruptorServer.Event> dispatchQ = new ArrayBlockingQueue<>(totalEvents);
        BlockingQueue<DisruptorServer.Event> joinerQ = new ArrayBlockingQueue<>(totalEvents);

        DisruptorServer disruptorServer = new DisruptorServer();
        disruptorServer.setDebugEnabled(false);
        disruptorServer.setDispatchCallback((ev) -> {
            try {
                dispatchQ.put(ev);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        disruptorServer.setProduceCallback((ev) -> {
            try {
                productionQ.put(ev);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        disruptorServer.setJoinCallback((ev) -> {
            try {
                joinerQ.put(ev);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        disruptorServer.setShutdownCallback(() -> {
            Assert.assertEquals(totalEvents, productionQ.size());
            Assert.assertEquals(totalEvents, dispatchQ.size());
            Assert.assertEquals(totalEvents, joinerQ.size());

            final int halfEvents = totalEvents/2;

            DisruptorServer.Event[] abDispatchedEvents = new DisruptorServer.Event[halfEvents];
            DisruptorServer.Event[] abJoinedEvents = new DisruptorServer.Event[halfEvents];
            DisruptorServer.Event[] cdDispatchedEvents = new DisruptorServer.Event[halfEvents];
            DisruptorServer.Event[] cdJoinedEvents = new DisruptorServer.Event[halfEvents];
            DisruptorServer.Event event;
            int c1 = 0, c2 = 0;
            while (!dispatchQ.isEmpty()) {
                event = dispatchQ.poll();
                if(event.getValue().startsWith("A") || event.getValue().startsWith("B")) {
                    abDispatchedEvents[c1++] = event;
                } else if(event.getValue().startsWith("C") || event.getValue().startsWith("D")) {
                    cdDispatchedEvents[c2++] = event;
                }
            }
            c1 = 0;
            c2 = 0;
            while (!joinerQ.isEmpty()) {
                event = joinerQ.poll();
                if(event.getValue().startsWith("A") || event.getValue().startsWith("B")) {
                    abJoinedEvents[c1++] = event;
                } else if(event.getValue().startsWith("C") || event.getValue().startsWith("D")) {
                    cdJoinedEvents[c2++] = event;
                }
            }
            Assert.assertEquals(halfEvents, abDispatchedEvents.length);
            Assert.assertEquals(halfEvents, cdDispatchedEvents.length);
            Assert.assertEquals(halfEvents, abJoinedEvents.length);
            Assert.assertEquals(halfEvents, cdJoinedEvents.length);

            Assert.assertArrayEquals(abDispatchedEvents, abJoinedEvents);
            Assert.assertArrayEquals(cdDispatchedEvents, cdJoinedEvents);

            /*System.out.println("################################");
            System.out.println("##### Assertion successful #####");
            System.out.println("##### All tests passed     #####");
            System.out.println("################################");*/
        });
        return disruptorServer;
    }
}
