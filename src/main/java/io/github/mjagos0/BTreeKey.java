package io.github.mjagos0;

import java.util.Arrays;
import java.util.Comparator;

public class BTreeKey {
    final byte[] key;
    final byte[] value;

    BTreeKey(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public static final Comparator<BTreeKey> LEXICOGRAPHIC_COMPARATOR =
            (a, b) -> Arrays.compareUnsigned(a.key, b.key);

    public int compareTo(byte[] otherKey) {
        return Arrays.compareUnsigned(this.key, otherKey);
    }

    public final byte[] getValue() {
        return value;
    }
}
