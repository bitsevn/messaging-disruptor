package com.bitsevn.projects.messaging.disruptor;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BenchmarkRunner {

    public static void main(String[] args) throws IOException, RunnerException {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        @Param({ "1024", "4096", "32768" })
        public String RING_SIZE;

        @Param({ "100", "1000", "10000", "100000" })
        public String EVENTS_PER_PRODUCER;

        @Param({ "8", "16", "32" })
        public int WORKERS;

        public List<String> PRODUCERS = Arrays.asList("A", "B", "C", "D");

        public DisruptorServer disruptorServer;

        @Setup
        public void setUp() {
            disruptorServer = new DisruptorServer();
            disruptorServer.setDebugEnabled(false);
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1, jvmArgsAppend = { "-Xms128M", "-Xmx2G" })
    @Measurement(iterations = 2)
    @Warmup(iterations = 3)
    @BenchmarkMode(Mode.Throughput)
    public void benchmarkThroughput(ExecutionPlan plan) {
        plan.disruptorServer.start(
            Integer.parseInt(plan.RING_SIZE),
            Integer.parseInt(plan.EVENTS_PER_PRODUCER),
            plan.WORKERS,
            plan.PRODUCERS
        );
    }
}
