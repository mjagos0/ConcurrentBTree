package io.github.mjagos0;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BTreeNode {
    final BTreeKey[] keys;
    final BTreeNode[] children;
    int numKeys;
    boolean isLeaf;
    final ReentrantReadWriteLock RWlock;
    private static final Logger logger = LogManager.getLogger(BTreeNode.class);

    /**
     * Insert byte[] value under byte[] key
     */
    BTreeNode(int m, boolean isLeaf) {
        children = new BTreeNode[m + 1];
        keys = new BTreeKey[m - 1 + 1]; // + 1 extra for splitting
        numKeys = 0;
        this.isLeaf = isLeaf;
        RWlock = new ReentrantReadWriteLock();
    }

    /**
     * Insert byte[] value under byte[] key
     */
    public int insertKey(BTreeKey key) {
        int i = binarySearch(key.getValue());

        // Shift larger keys to right
        for (int j = numKeys; j > i; j--) {
            keys[j] = keys[j - 1];
        }

        // Insert key
        keys[i] = key;
        numKeys++;

        return i;
    }

    public BTreeNode split(BTreeNode parent, int m) {
        BTreeNode right = new BTreeNode(m, isLeaf);
        right.RWlock.writeLock().lock();
        logger.debug("[{}] Created node ({}) and acquired lock", System.nanoTime(), right.hashCode());

        if (parent == null) { // = Root split
            parent = new BTreeNode(m, false);
            logger.debug("[{}] Created new root ({})", System.nanoTime(), parent.hashCode());
            parent.children[0] = this;
        }

        // Push median key to parent
        int median = (m - 1) / 2;
        int i = parent.insertKey(keys[median]);
        keys[median] = null;

        // Shift parent's children to the right and attach new node
        for (int j = parent.numKeys; j > i + 1; j--) {
            parent.children[j] = parent.children[j - 1];
        }
        parent.children[i + 1] = right;

        // Push right-most keys to right node
        for (int j = 0; j != numKeys - median - 1; j++) {
            right.keys[j] = keys[median + j + 1];
            keys[median + j + 1] = null;
        }

        // Push children to right node
        if (!right.isLeaf) {
            for (int j = 0; j < numKeys - median; j++) {
                right.children[j] = children[median + j + 1];
                children[median + j + 1] = null;
            }
        }

        // Synchronize numKeys
        right.numKeys = numKeys - median - 1;
        numKeys = median;

        // Release right lock
        right.RWlock.writeLock().unlock();
        logger.debug("[{}] Unlocked node ({})", System.nanoTime(), right.hashCode());

        return parent;
    }

    public boolean needsSplit() {
        return numKeys == keys.length;
    }

    public BTreeNode descend(int childIdx) {
        return children[childIdx];
    }

    public int binarySearch(byte[] key) {
        int low = 0, high = numKeys - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = keys[mid].compareTo(key);

            if (cmp == 0) {
                return mid;
            } else if (cmp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return low;
    }

}