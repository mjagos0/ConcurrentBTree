# Concurrent B-Tree
Custom **thread-safe B-tree** implementation in Java with `byte[]` keys and values.

## Description and methodology
Each node is guarded with `ReentrantReadWriteLock`. 
- Each node is owned exclusively by a single writer, or by group of readers.
- Both readers and writers always acquire locks downwards. They never ascend the tree, unless they own nodes in the path. This makes it **impossible to form deadlocks**.
- Both readers and writers use **lock coupling** to descend a tree - parent and child locks must be held in order to descend.

### Writers (PUT)
**Problem**: 
- Writer must ascend the tree during **node splitting**.
- Node **splitting can cascade through the entire tree**, up to the root.
- The **writer cannot acquire locks that are upwards**, as that may lead to deadlock with another descending thread.
- It is undesirable for a writer to hold locks to all nodes on the path, as that would lock out all other threads from the root and resulted in single-threaded execution.

**Idea**:
- Writers only hold locks to nodes which could split
- If the writer encounters a node that has space for at least 1 key, it is safe to release all predecessors above him
- Edge case: If all nodes on a path are full, writer needs to holds locks too all of them, as the split could cascade through the entire tree.

Together, this allows for multi-threaded, deadlock-free access.

### Readers (GET)
Can utilize lock-coupling - it is never necessary to hold more than 2 locks at the same time, as readers never split a node.

## How to Run
```bash
# Compile
mvn clean compile

# Run
mvn exec:java -Dexec.mainClass="io.github.mjagos0.Main"

# Test of concurrent execution
mvn test
```

### Benchmark
Bear in mind that although this implementation is thread-safe, the performance actually degrades with more than 4 threads.
According to the profiler, this is mostly due to lock contention.
