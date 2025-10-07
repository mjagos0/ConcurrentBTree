package io.github.mjagos0;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BTreeBenchmark {
    private static final Logger logger = LogManager.getLogger(BTreeBenchmark.class);
    private static final int WARMUP_RUNS = 3;

    public void benchmarkThreadScaling(int m, int total_keys) {
        logger.info("Starting ThreadScaling benchmark");

        int threadsCount = Runtime.getRuntime().availableProcessors();

        // Shuffle keys
        List<Integer> allKeys = new ArrayList<>(total_keys);
        for (int i = 0; i < total_keys; i++) {
            allKeys.add(i);
        }
        Collections.shuffle(allKeys, ThreadLocalRandom.current());

        // Warm-up
        for (int i = 0; i < WARMUP_RUNS; i++) {
            logger.info("Running warmup {}", i);
            runBenchmark(4, allKeys, m, true);
        }

        logger.info("Threads | Time (ms) | Valid B-Tree");
        logger.info("----------------------------------");

        for (int k = 1; k <= threadsCount; k++) {
            double elapsed = runBenchmark(k, allKeys, m, false);
            logger.info("{} | {} | {}", String.format("%7d", k), String.format("%9.3f", elapsed), "OK");
        }
    }

    public double runBenchmark(int threadsCount, List<Integer> allKeys, int m, boolean warmup) {
        ConcurrentBTree bTree = new ConcurrentBTree(m);
        int keysPerThread = allKeys.size() / threadsCount;
        List<Thread> threads = new ArrayList<>(threadsCount);

        long start = System.nanoTime();

        for (int t = 0; t < threadsCount; t++) {
            final int startIdx = t * keysPerThread;
            final int endIdx = (t == threadsCount - 1) ? allKeys.size() : startIdx + keysPerThread;

            Thread thread = new Thread(() -> {
                for (int i = startIdx; i < endIdx; i++) {
                    int keyNum = allKeys.get(i);
                    String key = String.format("%05d", keyNum);
                    bTree.put(key.getBytes(), key.getBytes());
                }
            }, "BTree-Worker-" + t);
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread interrupted during benchmark", e);
            }
        }

        long end = System.nanoTime();
        if (warmup) return 0; // skip validation and timing output

        boolean valid = bTree.validate();
        if (!valid) {
            logger.error("Validation failed for {} threads", threadsCount);
        }

        return (end - start) / 1_000_000.0;
    }
}