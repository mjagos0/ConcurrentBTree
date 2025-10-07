package io.github.mjagos0;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConcurrentBTree {
    private static final Logger logger = LogManager.getLogger(ConcurrentBTree.class);
    private final AtomicReference<BTreeNode> root;
    int m;

    ConcurrentBTree(int m) {
        this.m = m;
        this.root = new AtomicReference<>(new BTreeNode(m, true));
    }

    public void put(byte[] key, byte[] val) {
        BTreeKey newKey = new BTreeKey(key, val);
        ArrayDeque<BTreeNode> lockChain = new ArrayDeque<>();

        // Acquire root lock
        BTreeNode currentRoot;
        while (true) {
            currentRoot = root.get();
            currentRoot.RWlock.writeLock().lock();
            if (currentRoot == root.get()) {
                break; // Still valid
            }
            currentRoot.RWlock.writeLock().unlock(); // Try again
        }
        logger.debug("[{}] Locked root ({})", System.nanoTime(), currentRoot.hashCode());
        lockChain.addFirst(currentRoot);
        logger.debug("[{}] Added root ({}) to lockChain", System.nanoTime(), root.hashCode());

        // Descend to leaf
        BTreeNode node = lockChain.getFirst();
        while (!node.isLeaf) {
            int i = node.binarySearch(key);
            BTreeNode descendant = node.descend(i);
            descendant.RWlock.writeLock().lock();
            logger.debug("[{}] Locked node ({})", System.nanoTime(), descendant.hashCode());

            lockChain.addFirst(descendant);
            logger.debug("[{}] Added node ({}) to lockChain", System.nanoTime(), descendant.hashCode());

            if (descendant.numKeys < m - 1) {
                releaseAncestors(lockChain);
            }
            node = descendant;
        }

        // Insert key
        node.insertKey(newKey);

        // Cascade split through lockChain if necessary
        while (!lockChain.isEmpty()) {
            node = lockChain.removeFirst();
            logger.debug("[{}] Removed node ({}) from lockChain", System.nanoTime(), node.hashCode());
            if (node.needsSplit()) {
                if (root.get() == node) {
                    BTreeNode newRoot = node.split(null, m);
                    root.set(newRoot);
                } else {
                    node.split(lockChain.getFirst(), m);
                }
            }
            node.RWlock.writeLock().unlock();
            logger.debug("[{}] Unlocked node ({})", System.nanoTime(), node.hashCode());
        }
    }

    public byte[] get(byte[] key) {
        BTreeNode node;

        // Acquire root lock
        while (true) {
            node = root.get();
            node.RWlock.readLock().lock();
            if (node == root.get()) {
                break;
            }
            node.RWlock.readLock().unlock();
        }
        logger.debug("[{}] Locked root ({})", System.nanoTime(), node.hashCode());

        while (!node.isLeaf) {
            int i = node.binarySearch(key);

            if (i < node.numKeys && node.keys[i].compareTo(key) == 0) {
                byte[] value = node.keys[i].getValue();
                node.RWlock.readLock().unlock();
                return value;
            }

            BTreeNode descendant = node.descend(i);
            descendant.RWlock.readLock().lock();
            logger.debug("[{}] Locked node ({})", System.nanoTime(), descendant.hashCode());
            node.RWlock.readLock().unlock();
            logger.debug("[{}] Unlocked node ({})", System.nanoTime(), node.hashCode());
            node = descendant;
        }

        int i = node.binarySearch(key);
        if (i < node.numKeys && node.keys[i].compareTo(key) == 0) {
            byte[] value = node.keys[i].getValue();
            node.RWlock.readLock().unlock();
            return value;
        }
        node.RWlock.readLock().unlock();
        return null;
    }

    private void releaseAncestors(ArrayDeque<BTreeNode> lockChain) {
        while (lockChain.size() > 1) {
            BTreeNode node = lockChain.getLast();
            node.RWlock.writeLock().unlock();
            logger.debug("[{}] Unlocked node ({})", System.nanoTime(), node.hashCode());

            lockChain.removeLast();
            logger.debug("[{}] Removed node ({}) from lockChain", System.nanoTime(), node.hashCode());
        }
    }

    public void printTree() {
        ArrayDeque<BTreeNode> queue = new ArrayDeque<>();
        queue.add(root.get());

        int level = 0;
        while (!queue.isEmpty()) {
            int size = queue.size();
            System.out.print("Level " + level + ": ");

            for (int i = 0; i < size; i++) {
                BTreeNode node = queue.poll();
                System.out.print("[");
                for (int j = 0; j < node.numKeys; j++) {
                    String keyStr = new String(node.keys[j].key, StandardCharsets.UTF_8);
                    System.out.print(keyStr);
                    if (j < node.numKeys - 1) System.out.print(", ");
                }
                System.out.print("] ");

                if (!node.isLeaf) {
                    for (int j = 0; j <= node.numKeys; j++) {
                        if (node.children[j] != null) {
                            queue.add(node.children[j]);
                        }
                    }
                }
            }

            System.out.println();
            level++;
        }
        System.out.println();
    }

    public boolean validate() {
        BTreeNode r = getRoot();
        if (r == null) return false;

        int[] leafDepth = {-1};
        try {
            validateNode(r, null, null, 0, leafDepth);
            return true;
        } catch (IllegalStateException e) {
            logger.warn("BTree validation failed: {}", e.getMessage());
            return false;
        }
    }

    private void validateNode(BTreeNode node, BTreeKey min, BTreeKey max,
                              int depth, int[] leafDepth) {
        int m = getM();
        int minKeys = node.isLeaf ? 0 : (node == getRoot() ? 1 : (int) Math.ceil(m / 2.0) - 1);
        int maxKeys = m - 1;

        if (node.numKeys < minKeys || node.numKeys > maxKeys)
            throw new IllegalStateException("Invalid key count at depth " + depth);

        // Keys sorted within node
        for (int i = 1; i < node.numKeys; i++) {
            if (node.keys[i - 1].compareTo(node.keys[i].key) >= 0)
                throw new IllegalStateException("Keys not sorted at depth " + depth);
        }

        // Keys within parent's min/max range
        if (min != null && node.keys[0].compareTo(min.key) <= 0)
            throw new IllegalStateException("Node key below parent's min at depth " + depth);
        if (max != null && node.keys[node.numKeys - 1].compareTo(max.key) >= 0)
            throw new IllegalStateException("Node key above parent's max at depth " + depth);

        if (node.isLeaf) {
            if (leafDepth[0] == -1) leafDepth[0] = depth;
            else if (leafDepth[0] != depth)
                throw new IllegalStateException("Leaves at different depths");
            return;
        }

        // Check each child and its key range
        for (int i = 0; i <= node.numKeys; i++) {
            BTreeNode child = node.children[i];
            if (child == null)
                throw new IllegalStateException("Null child pointer at depth " + depth);

            // Determine valid range for this child
            BTreeKey childMin = (i == 0) ? min : node.keys[i - 1];
            BTreeKey childMax = (i == node.numKeys) ? max : node.keys[i];

            // Verify that all keys in child are within [childMin, childMax]
            if (child.numKeys > 0) {
                BTreeKey leftMost = child.keys[0];
                BTreeKey rightMost = child.keys[child.numKeys - 1];
                if (childMin != null && leftMost.compareTo(childMin.key) <= 0)
                    throw new IllegalStateException("Child key below lower bound at depth " + depth);
                if (childMax != null && rightMost.compareTo(childMax.key) >= 0)
                    throw new IllegalStateException("Child key above upper bound at depth " + depth);
            }

            validateNode(child, childMin, childMax, depth + 1, leafDepth);
        }
    }

    public BTreeNode getRoot() {
        return root.get();
    }

    public int getM() {
        return m;
    }
}