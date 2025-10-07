package io.github.mjagos0;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        int K = 8;               // Number of threads
        int M = 3;               // Order of the tree, max children for node = M, max keys per node = M - 1
        int TOTAL_KEYS = 100;    // Number of keys to insert

        // Construct a small B-tree M = 3 with 100 keys split among 8 threads
        ConcurrentBTree bTree = new ConcurrentBTree(M);
        List<Thread> threads = new ArrayList<>();

        long startTime = System.nanoTime();

        for (int t = 0; t < K; t++) {
            final int start = t * (TOTAL_KEYS / K);
            final int end = (t == K - 1) ? TOTAL_KEYS : start + (TOTAL_KEYS / K);
            Thread thread = new Thread(() -> {
                for (int i = start; i < end; i++) {
                    String key = String.format("%04d", i);
                    bTree.put(key.getBytes(), key.getBytes());
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Thread interrupted while waiting for completion", e);
            }
        }

        long endTime = System.nanoTime();
        double elapsedMs = (endTime - startTime) / 1_000_000.0;
        logger.info("Built tree with {} keys using {} threads in {} ms",
                TOTAL_KEYS, K, String.format("%.3f", elapsedMs));

        bTree.printTree();
        logger.info("BTree validation result: {}", bTree.validate());

        // Thread-scaling benchmark
        BTreeBenchmark bt = new BTreeBenchmark();
        bt.benchmarkThreadScaling(3, 100_000);
    }
}
