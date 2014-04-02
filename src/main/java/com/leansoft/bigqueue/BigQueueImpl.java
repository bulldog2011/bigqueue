package com.leansoft.bigqueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;


/**
 * A big, fast and persistent queue implementation.
 * <p/>
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 *
 * @author bulldog
 */
public class BigQueueImpl implements IBigQueue {

    final IBigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    // front index of the big queue,
    final AtomicLong queueFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    IMappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();
    final Lock futureLock = new ReentrantLock();
    private SettableFuture<IBigQueue> dequeueFuture;

    /**
     * A big, fast and persistent queue implementation,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName) throws IOException {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName, int pageSize) throws IOException {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);

        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl) innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER,
                10 * 1000/*does not matter*/);
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);
    }

    @Override
    public boolean isEmpty() {
        return this.queueFrontIndex.get() == this.innerArray.getHeadIndex();
    }

    @Override
    public void enqueue(byte[] data) throws IOException {
        this.innerArray.append(data);

        this.completeFuture();
    }


    /**
     * Completes the dequeue future
     */
    private void completeFuture() {
        futureLock.lock();
        if (dequeueFuture != null) {
            dequeueFuture.set(this);
        }
        futureLock.unlock();
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializeFutureIfNecessary() {
        futureLock.lock();
        if (dequeueFuture == null) {
            dequeueFuture = SettableFuture.create();
        }
        if (!this.isEmpty()) {
            dequeueFuture.set(this);
        }
        futureLock.unlock();
    }

    /**
     * Resets the future to null
     */
    private void invalidateFuture() {
        futureLock.lock();
        dequeueFuture = null;
        futureLock.unlock();
    }

    @Override
    public byte[] dequeue() throws IOException {
        long queueFrontIndex = -1L;
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return null;
            }
            queueFrontIndex = this.queueFrontIndex.get();
            byte[] data = this.innerArray.get(queueFrontIndex);
            long nextQueueFrontIndex = queueFrontIndex;
            if (nextQueueFrontIndex == Long.MAX_VALUE) {
                nextQueueFrontIndex = 0L; // wrap
            } else {
                nextQueueFrontIndex++;
            }
            this.queueFrontIndex.set(nextQueueFrontIndex);
            // persist the queue front
            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(nextQueueFrontIndex);
            queueFrontIndexPage.setDirty(true);

            if (this.isEmpty()) {
                this.invalidateFuture();
            }

            return data;
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public ListenableFuture<IBigQueue> queueReadyForDequeue() {
        initializeFutureIfNecessary();
        return dequeueFuture;

    }

    @Override
    public void removeAll() throws IOException {
        try {
            queueFrontWriteLock.lock();
            this.innerArray.removeAll();
            this.queueFrontIndex.set(0L);
            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(0L);
            queueFrontIndexPage.setDirty(true);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public byte[] peek() throws IOException {
        if (this.isEmpty()) {
            return null;
        }
        byte[] data = this.innerArray.get(this.queueFrontIndex.get());
        return data;
    }

    /**
     * apply an implementation of a ItemIterator interface for each queue item
     *
     * @param iterator
     * @throws IOException
     */
    @Override
    public void applyForEach(ItemIterator iterator) throws IOException {
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return;
            }

            long index = this.queueFrontIndex.get();
            for (long i = index; i < this.innerArray.size(); i++) {
                iterator.forEach(this.innerArray.get(i));
            }
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (this.queueFrontIndexPageFactory != null) {
            this.queueFrontIndexPageFactory.releaseCachedPages();
        }

        if (dequeueFuture != null) {
            dequeueFuture.cancel(false);
        }

        this.innerArray.close();
    }

    @Override
    public void gc() throws IOException {
        long beforeIndex = this.queueFrontIndex.get();
        if (beforeIndex == 0L) { // wrap
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }
        try {
            this.innerArray.removeBeforeIndex(beforeIndex);
        } catch (IndexOutOfBoundsException ex) {
            // ignore
        }
    }

    @Override
    public void flush() {
        try {
            queueFrontWriteLock.lock();
            this.queueFrontIndexPageFactory.flush();
            this.innerArray.flush();
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public long size() {
        long qFront = this.queueFrontIndex.get();
        long qRear = this.innerArray.getHeadIndex();
        if (qFront <= qRear) {
            return (qRear - qFront);
        } else {
            return Long.MAX_VALUE - qFront + 1 + qRear;
        }
    }

}
