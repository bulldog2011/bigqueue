package com.leansoft.bigqueue;

import java.io.Closeable;
import java.io.IOException;

/**
 * FanOut queue ADT
 * 
 * @author bulldog
 *
 */
public interface IFanOutQueue extends Closeable {
	
	/**
	 * Determines whether a fanout queue is empty
	 * 
	 * @param fid the fanout identifier
	 * @return true if empty, flase otherwise
	 */
	public boolean isEmpty(String fid) throws IOException;
	
	/**
	 * Adds an item at the back of the queue
	 * 
	 * @param data to be enqueued data
	 * @throws IOException exception throws if there is any IO error during enqueue operation.
	 */
	public void enqueue(byte[] data)  throws IOException;
	
	/**
	 * Retrieves and removes the front of a queue
	 * 
	 * @param fid the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	public byte[] dequeue(String fid) throws IOException;
	
	/**
	 * Peek the item at the front of a queue, without removing it from the queue
	 * 
	 * @param fid the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	public byte[] peek(String fid)  throws IOException;
	
	
	/**
	 * Peek the length of the item at the front of a queue
	 * 
	 * @param fid the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	public int peekLength(String fid) throws IOException;
	
	/**
	 * Peek the timestamp of the item at the front of a queue
	 * 
	 * @param fid the fanout identifier
	 * @return data at the front of a queue
	 * @throws IOException exception throws if there is any IO error during peek operation.
	 */
	public long peekTimestamp(String fid) throws IOException;
	
	/**
	 * Total number of items remaining in the queue
	 * 
	 * @param fid the fanout identifier
	 * @return total number
	 */
	public long size(String fid) throws IOException ;
	
	/**
	 * Force to persist current state of the queue, 
	 * 
	 * normally, you don't need to flush explicitly since:
	 * 1.) FanOutQueue will automatically flush a cached page when it is replaced out,
	 * 2.) FanOutQueue uses memory mapped file technology internally, and the OS will flush the changes even your process crashes,
	 * 
	 * call this periodically only if you need transactional reliability and you are aware of the cost to performance.
	 */
	public void flush();
	
	/**
	 * Remove all data before specific timestamp, truncate back files and advance the queue front if necessary.
	 * 
	 * @param timestamp a timestamp
	 * @throws IOException exception thrown if there was any IO error during the removal operation
	 */
	void removeBefore(long timestamp) throws IOException;
	
	/**
	 * Limit the back file size of this queue, truncate back files and advance the queue front if necessary.
	 * 
	 * Note, this is a best effort call, exact size limit can't be guaranteed
	 * 
	 * @param sizeLmit size limit
	 * @throws IOException exception thrown if there was any IO error during the operation
	 */
	void limitBackFileSize(long sizeLmit) throws IOException;
	
	/**
	 * Current total size of the back files of this queue
	 * 
	 * @return total back file size
	 * @throws IOException exception thrown if there was any IO error during the operation
	 */
	long getBackFileSize() throws IOException;
	
	
    /**
     * Find an index closest to the specific timestamp when the corresponding item was enqueued.
     * 
     * @param timestamp when the corresponding item was appended
     * @return an index
     * @throws IOException exception thrown during the operation
     */
    long findClosestIndex(long timestamp) throws IOException;
    
    /**
     * Reset the front index of specific fanout queue.
     * 
     * @param fid fanout identifier
     * @param index target index
     * @throws IOException exception thrown during the operation
     */
    void resetQueueFrontIndex(String fid, long index) throws IOException;
    
	/**
	 * Removes all items of a queue, this will empty the queue and delete all back data files.
	 * 
	 * @throws IOException exception throws if there is any IO error during dequeue operation.
	 */
	public void removeAll() throws IOException;

}
