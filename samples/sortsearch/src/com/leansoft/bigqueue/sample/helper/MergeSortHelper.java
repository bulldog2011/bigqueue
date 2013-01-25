package com.leansoft.bigqueue.sample.helper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;


public class MergeSortHelper {
	
	private static Random random = new Random();
	private static AtomicInteger tempQueueId = new AtomicInteger(0);
	
	// adjust this on your machine accordingly.
	public static String SAMPLE_DIR = "/bigqueue/sample/sortsearch/";
	
	private static String getNextTempQueueName() {
		return "tempq"+ tempQueueId.getAndDecrement();
	}
	
	/**
	 * Generate a random string with format : a random long + random string
	 * 
	 * @param size the size of random string
	 * @return a random string
	 */
	public static String genRandomString(int size) {
		String nextLongString = String.valueOf(random.nextLong());
		String randomString = RandomStringUtil.randomString(size - nextLongString.length());
		return nextLongString + randomString;
	}
	
	/**
	 * Populate a big queue with fixed size random string
	 * 
	 * @param bigQueue the big queue to populate
	 * @param queueSize the size of the big queue
	 * @param itemSize the size of item in queue
	 * @throws IOException exception thrown when there is IO error during population operation
	 */
	public static void populateBigQueue(IBigQueue bigQueue, long queueSize, int itemSize) throws IOException {
		for(long i = 0; i < queueSize; i++) {
			String randomString = genRandomString(itemSize);
			bigQueue.enqueue(randomString.getBytes());
		}
	}
	
	/**
	 * Sort a string array in memory.
	 * 
	 * @param stringArray the string array to be sorted.
	 */
	public static void inMemSort(String[] stringArray) {
		long begin = System.currentTimeMillis();
		Arrays.sort(stringArray);
		long end = System.currentTimeMillis();
		System.out.println("Time to sort " + stringArray.length + " in memory is " + (end - begin) + "ms.");
	}

	/**
	 * Divide a big queue into memory sortable sub-queues, sort these sub-queues in turn, and 
	 * return a queue with all sorted sub-queues.
	 * 
	 * This method is thread safe.
	 * 
	 * @param bigQueue the big queue to be sorted
	 * @param maxInMemSortNumOfItems max number of items that can be sorted in memory in one pass
	 * @param queueOfSortedQueues queue of sorted sub-queues
	 * @throws IOException exception thrown if there is IO error during queue making operation.
	 */
	public static void makeQueueOfSortedQueues(IBigQueue bigQueue, int maxInMemSortNumOfItems, Queue<IBigQueue> queueOfSortedQueues) throws IOException {
		List<String> list = new ArrayList<String>();
		
		while(true) {
			// continously extract items from big queue
			byte[] data = bigQueue.dequeue();
			if (data != null) {
				list.add(new String(data));
			}
			// if we get max number of items sortable in memory, then sort them and make a sub-queue
			if (list.size() == maxInMemSortNumOfItems || data == null) {
				long begin = System.currentTimeMillis();
				String[] stringArray = list.toArray(new String[0]);
				if (stringArray.length == 0) break;
				// sort in memory
				inMemSort(stringArray);
				String newQueueName = getNextTempQueueName();
				IBigQueue newBigQueue = new BigQueueImpl(SAMPLE_DIR, newQueueName);
				// make sorted sub-queue
				for(String item : stringArray) {
					newBigQueue.enqueue(item.getBytes());
				}
				// put the sub-queue into output queue
				queueOfSortedQueues.offer(newBigQueue);
				newBigQueue.close();
				
				if (data == null) break;
				list.clear();
				long end = System.currentTimeMillis();
				output("Time used to make one sorted queue " + (end - begin) + " ms, maxInMemSortNumOfItems = " + maxInMemSortNumOfItems);
			}
		}
	}
	
	/**
	 * N way merge sort using queues.
	 * 
	 * This method is thread safe
	 * 
	 * algorithm:
	 * 0. build a new queue as target queue,
	 * 1. peek items in all n way sorted queues to find out the lowest item,
	 * 2. consume and put the lowest item into the target queue,
	 * 3. repeat 1 & 2 until all items in n way sorted queues have been consumed.
	 * 4. return target queue.
	 * 
	 * @param listOfSortedQueue a list of sorted sub-queues
	 * @return target sorted queue
	 * @throws IOException exception thrown if there is IO error during the operation
	 */
	public static IBigQueue nWayMergeSort(List<IBigQueue> listOfSortedQueues) throws IOException {
		String newQueueName = getNextTempQueueName();
		IBigQueue targetBigQueue = new BigQueueImpl(SAMPLE_DIR, newQueueName); // target queue
		
		int ways = listOfSortedQueues.size();
		long begin = System.currentTimeMillis();
		while(true) {
			IBigQueue queueWithLowestItem = null;
			String lowestItem = null;
			// find the lowest item in all n way queues
			for(IBigQueue bigQueue : listOfSortedQueues) {
				if (!bigQueue.isEmpty()) {
					if (queueWithLowestItem == null) {
						queueWithLowestItem = bigQueue;
						lowestItem = new String(bigQueue.peek());
					} else {
						String item = new String(bigQueue.peek());
						if (item.compareTo(lowestItem) < 0) {
							queueWithLowestItem = bigQueue;
							lowestItem = item;
						}
					}
				}
			}
			if (queueWithLowestItem == null) break; // all queues are empty
			// extract and put the lowest item into the target queue
			byte[] data = queueWithLowestItem.dequeue();
			targetBigQueue.enqueue(data);
		}
		
		// release the source queues since we have done with them
		for(IBigQueue bigQueue : listOfSortedQueues) {
			bigQueue.removeAll(); // make empty the queue, delete back data files to save disk space
			bigQueue.close();
		}
		
		targetBigQueue.close();
		
		long end = System.currentTimeMillis();
		
		output("Time used to merge sort  " + ways + " way queues : " + (end - begin) + " ms.");
		
		return targetBigQueue;
	}
	
	/**
	 * Merge sort a queue of sorted queues,
	 * 
	 * This method is thread-safe
	 * 
	 * algorithm:
	 * 1. extract(poll) n queues from queueOfSortedQueues and put them into listOfSortedQueues, 2 <= n <= maxWays,
	 * 2. merge sort listOfSortedQueues using nWayMerageSort method above, and return the result queue into queueOfSortedQueue again,
	 * 3. repeat 1 & 2 until there is only one queue left in queueOfSortedQueues, that's the final sorted queue.
	 * 
	 * @param queueOfSortedQueues a queue of sorted sub-queues
	 * @param maxWays max allowed ways to merge sort
	 * @throws IOException exception thrown if there is IO error during the operation
	 */
	public static void mergeSort(Queue<IBigQueue> queueOfSortedQueues, int maxWays) throws IOException {
		List<IBigQueue> listOfSortedQueues = new ArrayList<IBigQueue>();
		// repeat until there is only one left in queueOfSortedQueue
		while(queueOfSortedQueues.size() > 1) {
			listOfSortedQueues.clear();
			int count = 0;
			while(!queueOfSortedQueues.isEmpty() && count < maxWays) {
				IBigQueue sortedQueue = queueOfSortedQueues.poll();
				if (sortedQueue != null) { // null only happen in multi-threads case
					listOfSortedQueues.add(sortedQueue);
					count++;
				}
			}
			
			if (listOfSortedQueues.size() > 1) { // grabbed enough to do n way mergesort
				// n way merge sort
				IBigQueue targetSortedQueue = nWayMergeSort(listOfSortedQueues);
				// return the result queue into queueOfSortedQueues
				queueOfSortedQueues.offer(targetSortedQueue);
			} else if (listOfSortedQueues.size() == 1) { // 1 only happen in multi-threads case
				// grabbed one, but can't do n way meragesort, so just return and try again
				queueOfSortedQueues.offer(listOfSortedQueues.remove(0));
			} else { // only happen in multi-threads case
				// grabbed nothing, retry
			}
		}
	}
	
	public static void output(String message) {
		System.out.println(new Date() + " - " + message);
	}
	
}
