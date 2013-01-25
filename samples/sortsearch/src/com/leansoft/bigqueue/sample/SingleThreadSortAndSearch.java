package com.leansoft.bigqueue.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigArray;
import com.leansoft.bigqueue.IBigQueue;
import com.leansoft.bigqueue.sample.helper.BinarySearchHelper;
import com.leansoft.bigqueue.sample.helper.MergeSortHelper;
/**
 * Sample to sort and search big data using big queue and big array
 * 
 * @author bulldog
 *
 */
public class SingleThreadSortAndSearch {
	
	// configurable parameters, adjust them according to your environment and requirements
	//////////////////////////////////////////////////////////////////
	// max number of items can be sorted in memory in a one pass
	static int maxInMemSortNumOfItems = 1024 * 1024 * 20;
	// max number of items to be sorted and searched
	static long maxNumOfItems = maxInMemSortNumOfItems * 32;
	// bytes per item
	static int itemSize = 100;
	// ways to merge sort in parallel, must >= 2;
	static int maxMergeSortWays = 4;
	//////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) throws IOException {
		MergeSortHelper.SAMPLE_DIR = MergeSortHelper.SAMPLE_DIR + "single_thread";

		// sort first
		mergeSort();
		
		// then search
		binarySearch();
	}
	
	static void mergeSort() throws IOException {
		MergeSortHelper.output("Single thread sort begin ...");
		MergeSortHelper.output("Generating random big queue ...");
		IBigQueue srcBigQueue = new BigQueueImpl(MergeSortHelper.SAMPLE_DIR, "srcq");
		MergeSortHelper.populateBigQueue(srcBigQueue, maxNumOfItems, itemSize);
		
		long start = System.currentTimeMillis();
		MergeSortHelper.output("Making queue of sorted queues ...");
		Queue<IBigQueue> queueOfSortedQueues = new LinkedList<IBigQueue>();
		MergeSortHelper.makeQueueOfSortedQueues(srcBigQueue, maxInMemSortNumOfItems, queueOfSortedQueues);
		// have done with source queue, empty it and delete back data files to save disk space
		srcBigQueue.removeAll();
		srcBigQueue.close();
		
		MergeSortHelper.output("Merging and sorting the queues ...");
		MergeSortHelper.mergeSort(queueOfSortedQueues, maxMergeSortWays);
		long end = System.currentTimeMillis();
		MergeSortHelper.output("Mergesort finished.");
		
		MergeSortHelper.output("Time used to sort " + maxNumOfItems + " string items is " + (end - start) + "ms");
		MergeSortHelper.output("Item size each is " + itemSize + " bytes");
		MergeSortHelper.output("Total size sorted " + (long)(maxNumOfItems * itemSize) / (1024 * 1024) + "MB");
		
		IBigQueue targetSortedQueue = queueOfSortedQueues.poll(); // last and only one is the target sorted queue
		
		MergeSortHelper.output("Validation begin ....");
		long targetSize = targetSortedQueue.size();
		if(targetSize != maxNumOfItems) {
    		System.err.println("target queue size is not correct!, target queue size is " + targetSize + " expected queue size is " + maxNumOfItems);
		}
		
		// first sorted item
		String previousItem = new String(targetSortedQueue.dequeue());
		
		// put sorted items in a big array so we can binary search it later
		// validate the sorted queue at the same time
		IBigArray bigArray = new BigArrayImpl(MergeSortHelper.SAMPLE_DIR, "sample_array");
		bigArray.append(previousItem.getBytes());
		for(int i = 1; i < targetSize; i++) {
			String item = new String(targetSortedQueue.dequeue());
			if (item.compareTo(previousItem) < 0) {
				System.err.println("target queue is not in sorted order!");
			}
			bigArray.append(item.getBytes());
			previousItem = item;
		}
		MergeSortHelper.output("Validation finished.");
		
		// have done with target sorted queue, empty it and delete back data files to save disk space
		targetSortedQueue.removeAll();
		targetSortedQueue.close();
		
		bigArray.close();
	}
	
	static void binarySearch() throws IOException {
		IBigArray bigArray = new BigArrayImpl(MergeSortHelper.SAMPLE_DIR, "sample_array"); 
		// randomly get some items which can be found later
		int searchLoopLimit = 10000;
		List<String> randomKeys = new ArrayList<String>();
		Random random = new Random();
		for(int i = 0; i < searchLoopLimit / 2; i++) {
			long randomIndex = (long)(random.nextDouble() * maxNumOfItems);
			String randomKey = new String(bigArray.get(randomIndex));
			randomKeys.add(randomKey);
		}
		bigArray.close(); // close to release cache, otherwise the performance of binary search will not be real.
		
		bigArray = new BigArrayImpl(MergeSortHelper.SAMPLE_DIR, "sample_array"); 
		
		MergeSortHelper.output("Single thread binary search begin ...");
		int foundCount = 0;
		long start = System.currentTimeMillis();
		for(int i = 0; i < searchLoopLimit; i++) {
			String randomKey = null;
			if (i < searchLoopLimit / 2) {
				randomKey = randomKeys.get(i);
			} else {
				randomKey = MergeSortHelper.genRandomString(itemSize);
			}
			long result = BinarySearchHelper.binarySearch(bigArray, randomKey, 0, bigArray.size() - 1);
			if (result != BinarySearchHelper.NOT_FOUND) {
				foundCount++;
			}
		}
		long end = System.currentTimeMillis();
		MergeSortHelper.output("Binary search finished.");
		MergeSortHelper.output("Time to search " + searchLoopLimit + " items in the big array is " + (end - start) + " ms.");
		MergeSortHelper.output("Average search time is " + (double)(end - start) / searchLoopLimit + "ms.");
		MergeSortHelper.output("Found count is " + foundCount);
		
		// have done with the big array, empty it and delete back data files to save disk space
		bigArray.removeAll();
		bigArray.close();
	}
}
