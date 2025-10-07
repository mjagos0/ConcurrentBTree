package io.github.mjagos0;

import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

public class ConcurrentTest {

    private static final int TOTAL_KEYS = 100_000;
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int M = 4; // B-tree order

    @Test
    void concurrentPutAndCheckKeys() throws InterruptedException {
        ConcurrentBTree bTree = new ConcurrentBTree(M);

        // Shuffle keys
        List<Integer> allKeys = new ArrayList<>(TOTAL_KEYS);
        for (int i = 0; i < TOTAL_KEYS; i++) {
            allKeys.add(i);
        }
        Collections.shuffle(allKeys, ThreadLocalRandom.current());

        int keysPerThread = TOTAL_KEYS / THREAD_COUNT;
        List<Thread> threads = new ArrayList<>(THREAD_COUNT);

        // Insert keys concurrently using plain Threads
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int startIdx = t * keysPerThread;
            final int endIdx = (t == THREAD_COUNT - 1) ? TOTAL_KEYS : startIdx + keysPerThread;

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

        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }

        // Validate B-tree structure
        assertTrue(bTree.validate(), "B-tree structure invalid after concurrent inserts");

        // Check all inserted keys exist
        for (int k = 0; k < TOTAL_KEYS; k++) {
            String key = String.format("%05d", k);
            byte[] value = bTree.get(key.getBytes());
            assertNotNull(value, "Missing key: " + key);
        }

        // Randomly check keys both inside and outside the inserted range
        for (int i = 0; i < 100_000; i++) {
            int k = ThreadLocalRandom.current().nextInt(0, TOTAL_KEYS * 2); // double the range
            String key = String.format("%05d", k);
            byte[] value = bTree.get(key.getBytes());

            boolean shouldExist = (k < TOTAL_KEYS);
            if (shouldExist) {
                assertNotNull(value, "Missing expected key: " + key);
            } else {
                assertNull(value, "Unexpected key found: " + key);
            }
        }
    }
}
