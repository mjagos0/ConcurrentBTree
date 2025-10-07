# Concurrent B-Tree
Custom **thread-safe B-tree** implementation in Java with `byte[]` keys and values.

## Description and methodology
Each node is guarded with `ReentrantReadWriteLock`. 
- Both readers and writers never ascend the tree, unless they ascend through a path of nodes they own lock to. This makes it **impossible to form deadlocks**.
- Both readers and writers use **lock coupling** to descend a tree - parent and child locks must be held in order to descend.

### Writers (PUT)
**Problem**: 
- Writer must ascend the tree during **node splitting**.
- Node **splitting can cascade through the entire tree**, up to the root.
- The **writer cannot acquire locks that are upwards**, which would lead to deadlocks.
- At the same time, it is undesirable for a writer to hold locks to the entire path, as that would lock the root, which would lock out other threads and result in single-threaded execution

**Idea**:
- Writers only hold locks to nodes which could split
- If the writer encounters a node that has space for at least 1 key, it is safe to release all predecessors above him
- The edge case is when writers path contains all nodes which are full, in which case, the writer needs to hold lock to the entire path

Together, this allows for multi-threaded, deadlock-free access.

### Readers (GET)
Can utilize lock-coupling - it is never necessary to hold more than 2 locks at the same time, as readers never split a node.

## How to Run
```bash
# Compile
mvn clean compile

# Run main benchmark
mvn exec:java -Dexec.mainClass="io.github.mjagos0.Main"

# Run tests
mvn test
```

### Benchmark
Bear in mind that although this implementation is thread-safe, the performance actually degrades with more than 4 threads.
According to the profiler, this is mostly due to lock contention.
