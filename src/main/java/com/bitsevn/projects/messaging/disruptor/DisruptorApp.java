package com.bitsevn.projects.messaging.disruptor;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class DisruptorApp {

    private static final boolean DEBUG = true;

    private AtomicInteger dispatchedStreams;
    private AtomicInteger joinedStreams;
    private AtomicInteger counter;

    public static void main(String[] args) {
        // must be power of two
        final int RING_BUFFER_SIZE = 4096;
        final int STREAMS_PER_GROUP = 100;
        // final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
        final int AVAILABLE_CORES = 8;
        final List<String> STREAM_GROUPS = Arrays.asList("A:AB", "B:AB", "C:CD", "D:CD");

        DisruptorApp app = new DisruptorApp();
        app.run(RING_BUFFER_SIZE, STREAMS_PER_GROUP, AVAILABLE_CORES, STREAM_GROUPS);

    }

    public void run(int RING_BUFFER_SIZE, int STREAMS_PER_GROUP, int AVAILABLE_CORES, List<String> STREAM_GROUPS) {

        int TOTAL_STREAMS = (STREAMS_PER_GROUP + 1) * STREAM_GROUPS.size();

        dispatchedStreams = new AtomicInteger(0);
        joinedStreams = new AtomicInteger(0);
        counter = new AtomicInteger(TOTAL_STREAMS);
        if(DEBUG) {
            System.out.println("----------------------------------------------");
            System.out.println("# RING_BUFFER_SIZE        : " + RING_BUFFER_SIZE);
            System.out.println("# STREAMS_PER_GROUP       : " + STREAMS_PER_GROUP);
            System.out.println("# WORKER_THREADS         : " + AVAILABLE_CORES);
            System.out.println("# STREAM_GROUPS           : " + STREAM_GROUPS);
            System.out.println("# TOTAL_STREAMS_PRODUCED  : " + TOTAL_STREAMS);
            System.out.println("# TOTAL_STREAMS_DISPATCHED: " + TOTAL_STREAMS);
            System.out.println("# TOTAL_STREAMS_JOINED    : " + TOTAL_STREAMS);
            System.out.println("----------------------------------------------");
        }

        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;

        int TOTAL_EVENT_HANDLERS = (AVAILABLE_CORES * 2) + STREAM_GROUPS.size() +  2; // work pool handlers for AB/CD, producers, dispatcher & joiner
        ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_EVENT_HANDLERS);

        // First stage stream pipeline
        Disruptor<Event> disruptorStage1 = new Disruptor<>(
                Event.EVENT_FACTORY,
                RING_BUFFER_SIZE,
                executorService,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        // Disruptor to create A/B stream pipeline
        Disruptor<Event> disruptorStageAB = new Disruptor<>(
                Event.EVENT_FACTORY,
                RING_BUFFER_SIZE,
                executorService,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        // Disruptor to create C/D stream pipeline
        Disruptor<Event> disruptorStageCD = new Disruptor<>(
                Event.EVENT_FACTORY,
                RING_BUFFER_SIZE,
                executorService,
                ProducerType.MULTI,
                new BusySpinWaitStrategy()
        );

        // Result joiner event handler
        EventHandler<Event> joinerHandler = (event, seq, batchEnd) -> {
            if(DEBUG) {
                System.out.println("[" + Thread.currentThread().getName() + "] [joiner-" + event.group + "] event " + event);
                joinedStreams.incrementAndGet();
                if(counter.decrementAndGet() <= 0) {
                    System.out.println("--------- Assertions ------------");
                    Assert.assertEquals(dispatchedStreams.get(), joinedStreams.get());
                    System.out.println("--------- Assertions ------------");
                    executorService.shutdownNow();
                }
            }
        };

        // handlers/consumers for A/B streams - will run in parallel (no. of workers = no. of available processor cores)
        // This best suits to BusySpinWaitStrategy as it is favourable for low latency systems
        disruptorStageAB.handleEventsWithWorkerPool(getWorkHandlers("AB", AVAILABLE_CORES)).then(joinerHandler);

        // handlers/consumers for A/B streams - will run in parallel (no. of workers = no. of available processor cores)
        // This best suits to BusySpinWaitStrategy as it is favourable for low latency systems
        disruptorStageCD.handleEventsWithWorkerPool(getWorkHandlers("CD", AVAILABLE_CORES)).then(joinerHandler);

        // handler for all streams (A/B/C/D) - works as an orchestrator/dispatcher/router -
        // distributes incoming streams to appropriate down streams
        EventHandler<Event> dispatcherHandler = getDispatcherEventHandler(disruptorStageAB, disruptorStageCD);

        // setup event handler for first stage
        disruptorStage1.handleEventsWith(dispatcherHandler);

        // kick start disruptor instances
        disruptorStage1.start();
        disruptorStageAB.start();
        disruptorStageCD.start();

        // now kick off produces for streams
        kickOffProducers(disruptorStage1, STREAM_GROUPS, STREAMS_PER_GROUP);

    }


    private EventHandler<Event> getDispatcherEventHandler(Disruptor<Event> downstreamAB, Disruptor<Event> downstreamCD) {
        return (event, seq, batchEnd) -> {
                if(DEBUG) {
                    System.out.println("[" + Thread.currentThread().getName() + "] [dispatcher-" + event.group + "] event " + event);
                    dispatchedStreams.incrementAndGet();
                }

                if(event.value.startsWith("A") || event.value.startsWith("B")) {
                    downstreamAB.publishEvent((e, s) -> {
                        e.value = event.value;
                        e.group = event.group;
                    });
                } else if(event.value.startsWith("C") || event.value.startsWith("D")){
                    downstreamCD.publishEvent((e, s) -> {
                        e.value = event.value;
                        e.group = event.group;
                    });
                }
            };
    }

    private void kickOffProducers(Disruptor<Event> DISRUPTOR, List<String> GROUPS, int STREAMS) {
        GROUPS.forEach(group -> {
            String[] params = group.split(":");
            new Thread(() -> {
                for(int i=1; i<=STREAMS; i++) {
                    final int num = i;
                    DISRUPTOR.publishEvent((event, seq) -> {
                        event.value = params[0] + num;
                        event.group = params[1];
                        if(DEBUG) {
                            System.out.println("[" + Thread.currentThread().getName() + "] [publisher-" + params[0] + "] event " + event);
                        }
                    });
                    int count = ThreadLocalRandom.current().nextInt(100);
                    while (count > 0) count--;// busy spin
                }

                // end of batch
                DISRUPTOR.publishEvent((event, seq) -> {
                    event.value = params[0] + "!";
                    event.group = params[1];
                    if(DEBUG) {
                        System.out.println("[" + Thread.currentThread().getName() + "] [publisher-" + params[0] + "] event " + event);
                    }
                });
            }, "producer-thread-" +  group).start();
        });
    }

    private WorkHandler<Event>[] getWorkHandlers(String GROUP, int MAX_WORKERS) {
        WorkHandler<Event>[] workHandlers = new WorkHandler[MAX_WORKERS];
        for(int i=0; i<MAX_WORKERS; i++) {
            final int worker = i + 1;
            workHandlers[i] = (event) -> {
                event.value = event.value + ":processed";
                if(DEBUG) {
                    System.out.println("[" + Thread.currentThread().getName() + "] [consumer-" + GROUP + "-thread-" + worker + "] event " + event);
                }
                int count = ThreadLocalRandom.current().nextInt(100);
                while (count > 0) count--;// busy spin
            };
        }
        return workHandlers;
    }

    static class Event {
        private String value;
        private String group;

        public Event() {}

        @Override
        public String toString() {
            return value;
        }

        public static final EventFactory<Event> EVENT_FACTORY = () -> new Event();
    }
}
