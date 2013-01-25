package com.leansoft.bigqueue.sample.helper;

import java.io.IOException;

import com.leansoft.bigqueue.IBigArray;

public class BinarySearchHelper {

	
    public static final long NOT_FOUND = -1;
	
    /**
     * Binary search a big array using classic binary search algorithm.
     * 
     * @param bigArray a big array to search
     * @param key target key
     * @param low the low index
     * @param high the high index
     * @return the target index if found, -1 otherwise.
     * @throws IOException
     */
    public static long binarySearch(IBigArray bigArray, String key, long low, long high) throws IOException {
    	if (low > high) {
    		return NOT_FOUND;
    	}
    	
    	long mid = (low + high) / 2;
    	
    	byte[] midData = bigArray.get(mid);
    	String midItem = new String(midData);
    	if (midItem.compareTo(key) < 0) {
    		return binarySearch(bigArray, key, mid + 1, high);
    	} else if (midItem.compareTo(key) > 0) {
    		return binarySearch(bigArray, key, low, mid - 1);
    	} else {
    		return mid;
    	}
    }

}
