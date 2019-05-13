package com.bitsevn.projects.messaging.disruptor.benchmark;

import com.bitsevn.projects.messaging.disruptor.DisruptorApp;
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
        public String RING_BUFFER_SIZE;

        @Param({ "100", "200", "300", "500", "1000" })
        public String STREAMS;

        @Param({ "4", "8", "16", "32" })
        public int WORKER_THREADS;

        public List<String> STREAM_GROUPS = Arrays.asList("A:AB", "B:AB", "C:CD", "D:CD");

        public DisruptorApp app;

        @Setup
        public void setUp() {
            app = new DisruptorApp();
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1, jvmArgsAppend = { "-Xms128M", "-Xmx2G" })
    @Measurement(iterations = 2)
    @Warmup(iterations = 3)
    @BenchmarkMode(Mode.Throughput)
    public void benchmarkThroughput(ExecutionPlan plan) {
        plan.app.run(
            Integer.parseInt(plan.RING_BUFFER_SIZE),
            Integer.parseInt(plan.STREAMS),
            plan.WORKER_THREADS,
            plan.STREAM_GROUPS
        );
    }
}
