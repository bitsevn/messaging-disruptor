package com.bitsevn.projects.messaging.disruptor;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DisruptorServer {

    private boolean debugEnabled = true;
    private MilestoneCallback produceCallback;
    private MilestoneCallback dispatchCallback;
    private MilestoneCallback joinCallback;
    private MilestoneCallback shutdownCallback;

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public void setDebugEnabled(boolean debugEnabled) {
        this.debugEnabled = debugEnabled;
    }

    public MilestoneCallback getProduceCallback() {
        return produceCallback;
    }

    public void setProduceCallback(MilestoneCallback produceCallback) {
        this.produceCallback = produceCallback;
    }

    public MilestoneCallback getDispatchCallback() {
        return dispatchCallback;
    }

    public void setDispatchCallback(MilestoneCallback dispatchCallback) {
        this.dispatchCallback = dispatchCallback;
    }

    public MilestoneCallback getJoinCallback() {
        return joinCallback;
    }

    public void setJoinCallback(MilestoneCallback joinCallback) {
        this.joinCallback = joinCallback;
    }

    public MilestoneCallback getShutdownCallback() {
        return shutdownCallback;
    }

    public void setShutdownCallback(MilestoneCallback shutdownCallback) {
        this.shutdownCallback = shutdownCallback;
    }

    interface MilestoneCallback {
        void onMilestone(Event e);
    }

    static class Event {
        private String value;
        private String result;

        public static final EventFactory<Event> EVENT_FACTORY = () -> new Event();

        public Event() {}

        public Event(String value) {
            this.value = value;
        }

        public Event(String value, String result) {
            this.value = value;
            this.result = result;
        }

        @Override
        public Event clone() {
            return new Event(value, result);
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public void clear() {
            this.value = null;
            this.result = null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;
            return Objects.equals(value, event.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public void start(int ringSize, int eventsPerProducer, int workers, List<String> producers) {
        final int totalEvents = producers.size() * eventsPerProducer;
        CountDownLatch gate = new CountDownLatch(totalEvents);
        Disruptor<Event> milestone = new Disruptor<>(
                Event.EVENT_FACTORY,
                ringSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BusySpinWaitStrategy());
        Disruptor<Event> milestoneAB = new Disruptor<>(
                Event.EVENT_FACTORY,
                ringSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE,
                new BusySpinWaitStrategy());
        Disruptor<Event> milestoneCD = new Disruptor<>(
                Event.EVENT_FACTORY,
                ringSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE,
                new BusySpinWaitStrategy());

        EventHandler<Event> dispatchHandler = (e, s, b) -> {
            if(dispatchCallback != null) dispatchCallback.onMilestone(e.clone());
            if(debugEnabled) System.out.println(String.format("[dispatcher] dispatch event %s", e));
            if(e.value.startsWith("A") || e.value.startsWith("B")) {
                milestoneAB.publishEvent((ev, sq) -> {
                    ev.value = e.value;
                    e.clear();
                });
            } else if(e.value.startsWith("C") || e.value.startsWith("D")){
                milestoneCD.publishEvent((ev, sq) -> {
                    ev.value = e.value;
                    e.clear();// clear at the end of the pipeline
                });
            }
        };

        EventHandler<Event> joinHandler = (e, s, b) -> {
            if(debugEnabled) System.out.println(String.format("[joiner] joining event %s", e));
            if(joinCallback != null) joinCallback.onMilestone(e.clone());
            e.clear();// clear at the end of the pipeline
            gate.countDown();
        };

        milestone.handleEventsWith(dispatchHandler);
        milestoneAB.handleEventsWithWorkerPool(getWorkerPool("ab", workers)).then(joinHandler);
        milestoneCD.handleEventsWithWorkerPool(getWorkerPool("cd", workers)).then(joinHandler);

        milestone.start();
        milestoneAB.start();
        milestoneCD.start();

        for(int p=0; p<producers.size(); p++) {
            final String producer = producers.get(p);
            new Thread(() -> {
                for(int i=0; i<eventsPerProducer; i++) {
                    final int eventId = i + 1;
                    final Event event = new Event(String.format("%s%s", producer, eventId));
                    milestone.publishEvent((e, s) -> e.value = event.value);
                    if(debugEnabled) System.out.println(String.format("[producer-%s] produced event %s", producer, event));
                    if(produceCallback != null) produceCallback.onMilestone(event.clone());
                    try {
                        TimeUnit.MICROSECONDS.sleep(ThreadLocalRandom.current().nextInt(20));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, String.format("producer-%s-thread", producer)).start();
        }

        try {
            gate.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(shutdownCallback != null) shutdownCallback.onMilestone(null);
            milestone.shutdown();
            milestoneAB.shutdown();
            milestoneCD.shutdown();
        }
    }

    private WorkHandler<Event>[] getWorkerPool(String producer, int workers) {
        WorkHandler<Event>[] workHandlers = new WorkHandler[workers];
        for(int i=0; i<workers; i++) {
            final int worker = i + 1;
            workHandlers[i] = (event) -> {
                event.result = "processed";
                if(debugEnabled) {
                    System.out.println( String.format("[%s-worker-%s] [%s] event %s", producer, worker, Thread.currentThread().getName(), event));
                }
                int count = ThreadLocalRandom.current().nextInt(100);
                while (count > 0) count--;// busy spin
            };
        }
        return workHandlers;
    }
}
