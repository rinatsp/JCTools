package org.jctools;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class SpmcLinkedArrayQueue<T> extends AbstractQueue<T> {
    final AtomicLong producerIndex;
    final AtomicReference<SpmcLinkedArrayQueue.ARA2> producerArray;

    final AtomicLong consumerIndex;
    SpmcLinkedArrayQueue.ARA2 consumerArray;
    int consumerOffset;

    final int maxOffset;

    static final Object ALLOCATING = new Object();

    public SpmcLinkedArrayQueue(int arrayCapacity) {
        int c = arrayCapacity;
        this.maxOffset = c - 1;
        SpmcLinkedArrayQueue.ARA2 array = new SpmcLinkedArrayQueue.ARA2(c + 1, 0);
        this.producerIndex = new AtomicLong();
        this.consumerIndex = new AtomicLong();
        this.producerArray = new AtomicReference<SpmcLinkedArrayQueue.ARA2>();
        this.consumerArray = array;
        this.producerArray.lazySet(array);
    }

    @Override
    public boolean offer(T value) {
        Objects.requireNonNull(value);

        final int m = maxOffset;

        SpmcLinkedArrayQueue.ARA2 array = producerArray.get();
        final long index = producerIndex.getAndIncrement();

        long start = array.start;
        long end = array.end;

        if (start - index > 0L) {
            throw new IllegalStateException(index + " vs. " + start);
        } else
        if (index < end) {
            int offset = (int)(index - start);
            array.lazySet(offset, value);
        } else
        if (index >= end) {
            for (;;) {
                Object nextArray = array.next();
                if (nextArray == null) {
                    if (array.casNext(null, ALLOCATING)) {
                        nextArray = new SpmcLinkedArrayQueue.ARA2(m + 2, end);
                        array.svNext(nextArray);
                    } else {
                        while ((nextArray = array.next()) == ALLOCATING);
                    }
                } else {
                    while ((nextArray = array.next()) == ALLOCATING);
                }

                SpmcLinkedArrayQueue.ARA2 nextArray2 = (SpmcLinkedArrayQueue.ARA2)nextArray;
                if (array.end < index) {
                    producerArray.compareAndSet(array, nextArray2);
                }
                array = nextArray2;

                start = end;
                end = array.end;
                if (index < end) {
                    break;
                }
            }
            int offset = (int)(index - start);
            array.lazySet(offset, value);
        }

        return true;
    }

    @Override
    public T poll() {
        final long index = consumerIndex.get();
        SpmcLinkedArrayQueue.ARA2 array = consumerArray;
        int offset = consumerOffset;
        final int m = maxOffset;
        if (offset > m) {
            for (;;) {
                Object next = array.next();
                if (next == ALLOCATING) {
                    continue;
                }
                if (next != null) {
                    offset = 0;
                    array = (SpmcLinkedArrayQueue.ARA2)next;
                    consumerArray = array;
                    break;
                }
                if (producerIndex.get() <= index) {
                    return null;
                }
            }
        }
        for (;;) {
            Object o = array.get(offset);
            if (o != null) {
                consumerOffset = offset + 1;
                consumerIndex.lazySet(index + 1);
                array.lazySet(offset, null);
                return (T)o;
            }
            if (producerIndex.get() <= index) {
                return null;
            }
        }
    }


    @Override
    public T peek() {
        final long index = consumerIndex.get();
        SpmcLinkedArrayQueue.ARA2 array = consumerArray;
        int offset = consumerOffset;
        final int m = maxOffset;
        if (offset > m) {
            for (;;) {
                Object next = array.next();
                if (next == ALLOCATING) {
                    continue;
                }
                if (next != null) {
                    offset = 0;
                    array = (SpmcLinkedArrayQueue.ARA2)next;
                    break;
                }
                if (producerIndex.get() <= index) {
                    return null;
                }
            }
        }
        for (;;) {
            Object o = array.get(offset);
            if (o != null) {
                return (T)o;
            }
            if (producerIndex.get() <= index) {
                return null;
            }
        }
    }


    @Override
    public boolean isEmpty() {
        return consumerIndex.get() == producerIndex.get();
    }

    @Override
    public int size() {
        long after = consumerIndex.get();
        for (;;) {
            final long before = after;
            final long pidx = producerIndex.get();
            after = consumerIndex.get();
            if (before == after) {
                return (int)(pidx - before);
            }
        }
    }

    static final class ARA2 extends AtomicReferenceArray<Object> {
        /** */
        private static final long serialVersionUID = -2977670280800260365L;
        public final long start;
        public final long end;
        final int nextOffset;
        public ARA2(int capacity, long start) {
            super(capacity);
            this.start = start;
            this.end = start + capacity - 1;
            this.nextOffset = capacity - 1;
        }
        public Object next() {
            return get(nextOffset);
        }
        public boolean casNext(Object expected, Object newValue) {
            return compareAndSet(nextOffset, expected, newValue);
        }
        public void svNext(Object newNext) {
            set(nextOffset, newNext);
        }
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }
}
