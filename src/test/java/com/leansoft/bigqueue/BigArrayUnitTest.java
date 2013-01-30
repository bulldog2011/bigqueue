package com.leansoft.bigqueue;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;

public class BigArrayUnitTest {
	
	private String testDir = TestUtil.TEST_BASE_DIR + "bigarray/unit";
	private IBigArray bigArray;

	@Test
	public void simpleTest() throws IOException {
	    bigArray = new BigArrayImpl(testDir, "simple_test");
		assertNotNull(bigArray);
		
		for(int i = 1; i <= 3; i++) {
			assertTrue(bigArray.getTailIndex() == 0L);
			assertTrue(bigArray.getHeadIndex() == 0L);
			assertTrue(bigArray.size() == 0L);
			assertTrue(bigArray.isEmpty());
			assertTrue(!bigArray.isFull());
			try {
				bigArray.get(0);
				fail("IndexOutOfBoundsException should be thrown here");
			} catch (IndexOutOfBoundsException ex) {
			}
			try {
				bigArray.get(1);
				fail("IndexOutOfBoundsException should be thrown here");
			} catch (IndexOutOfBoundsException ex) {
			}
			try {
				bigArray.get(Long.MAX_VALUE);
				fail("IndexOutOfBoundsException should be thrown here");
			} catch (IndexOutOfBoundsException ex) {
			}
			
			bigArray.append("hello".getBytes());
			assertTrue(bigArray.getTailIndex() == 0L);
			assertTrue(bigArray.getHeadIndex() == 1L);
			assertTrue(bigArray.size() == 1L);
			assertTrue(!bigArray.isEmpty());
			assertTrue(!bigArray.isFull());
			assertEquals("hello", new String(bigArray.get(0)));
			
			bigArray.flush();
			
			bigArray.append("world".getBytes());
			assertTrue(bigArray.getTailIndex() == 0L);
			assertTrue(bigArray.getHeadIndex() == 2L);
			assertTrue(bigArray.size() == 2L);
			assertTrue(!bigArray.isEmpty());
			assertTrue(!bigArray.isFull());
			assertEquals("hello", new String(bigArray.get(0)));
			assertEquals("world", new String(bigArray.get(1)));
			
			bigArray.removeAll();
		}
	}
	
	@Test 
	public void removeBeforeIndexTest() throws IOException {
	    bigArray = new BigArrayImpl(testDir, "remove_before_index_test");
		assertNotNull(bigArray);
		
		int loop = 5000000;
		for(int i = 0; i < loop; i++) {
			bigArray.append(("" + i).getBytes());
		}
		
		int half = loop / 2;
		bigArray.removeBeforeIndex(half);
		assertTrue(half == bigArray.getTailIndex());
		assertTrue(half == bigArray.size());
		assertEquals(half + "", new String(bigArray.get(half)));
		assertEquals(half + 1 + "", new String(bigArray.get(half + 1)));
		try {
			bigArray.get(half - 1);
			fail("IndexOutOfBoundsException should be thrown here");
		} catch (IndexOutOfBoundsException ex) {
		}
		
		long last = loop - 1;
		bigArray.removeBeforeIndex(last);
		assertTrue(last == bigArray.getTailIndex());
		assertTrue(1 == bigArray.size());
		assertEquals(last + "", new String(bigArray.get(last)));
		try {
			bigArray.get(last - 1);
			fail("IndexOutOfBoundsException should be thrown here");
		} catch (IndexOutOfBoundsException ex) {
		}
	}
	
	@Test 
	public void removeBeforeTest() throws IOException {
	    bigArray = new BigArrayImpl(testDir, "remove_before_test");
		assertNotNull(bigArray);
		
		int loop = 5000000;
		for(int i = 0; i < loop; i++) {
			bigArray.append(("" + i).getBytes());
		}
		
		int half = loop / 2;
		long halfTimestamp = bigArray.getTimestamp(half);
		this.bigArray.removeBefore(halfTimestamp);
		long tail = this.bigArray.getTailIndex();
		assertTrue(tail > 0);
		
	}
	
	@Test
	public void bigLoopTest() throws IOException {
	    bigArray = new BigArrayImpl(testDir, "big_loop_test");
		assertNotNull(bigArray);
		
		int loop = 1000000;
		long lastAppendTime = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			bigArray.append(("" + i).getBytes());
			assertTrue(bigArray.getTailIndex() == 0L);
			assertTrue(bigArray.getHeadIndex() == i + 1L);
			assertTrue(bigArray.size() == i + 1L);
			assertTrue(!bigArray.isEmpty());
			assertTrue(!bigArray.isFull());
			long currentTime = System.currentTimeMillis();
			long justAppendTime = bigArray.getTimestamp(i);
			assertTrue(justAppendTime <= currentTime);
			assertTrue(justAppendTime >=lastAppendTime);
			lastAppendTime = justAppendTime;
		}
		
		try {
			bigArray.get(loop);
			fail("IndexOutOfBoundsException should be thrown here");
		} catch (IndexOutOfBoundsException ex) {
		}
		
		assertTrue(bigArray.getTailIndex() == 0L);
		assertTrue(bigArray.getHeadIndex() == loop);
		assertTrue(bigArray.size() == loop);
		assertTrue(!bigArray.isEmpty());
		assertTrue(!bigArray.isFull());
		bigArray.close();
		
		// create a new instance on exiting array
	    bigArray = new BigArrayImpl(testDir, "big_loop_test");
		assertTrue(bigArray.getTailIndex() == 0L);
		assertTrue(bigArray.getHeadIndex() == loop);
		assertTrue(bigArray.size() == loop);
		assertTrue(!bigArray.isEmpty());
		assertTrue(!bigArray.isFull());
		
		lastAppendTime = System.currentTimeMillis();
		for(long i = 0; i < 10; i++) {
			bigArray.append(("" + i).getBytes());
			long currentTime = System.currentTimeMillis();
			long justAppendTime = bigArray.getTimestamp(loop + i);
			assertTrue(justAppendTime <= currentTime);
			assertTrue(justAppendTime >=lastAppendTime);
			lastAppendTime = justAppendTime;
			assertEquals(i + "", new String(bigArray.get(loop + i)));
		}
		assertTrue(bigArray.getTailIndex() == 0L);
		assertTrue(bigArray.getHeadIndex() == loop + 10);
		assertTrue(bigArray.size() == loop + 10);
		assertTrue(!bigArray.isEmpty());
		assertTrue(!bigArray.isFull());
	}
	
	@Test
	public void loopTimingTest() throws IOException {
	    bigArray = new BigArrayImpl(testDir, "loop_timing_test");
		assertNotNull(bigArray);
		
		int loop = 1000000;
		long begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			bigArray.append(("" + i).getBytes());
		}
		long end = System.currentTimeMillis();
		int timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Time used to sequentially append " + loop + " items : " + timeInSeconds + " seconds.");
		
		begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			assertEquals("" + i, new String(bigArray.get(i)));
		}
		end = System.currentTimeMillis();
		timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Time used to sequentially read " + loop + " items : " + timeInSeconds + " seconds.");
		
		begin = System.currentTimeMillis();
		List<Integer> list = new ArrayList<Integer>();
		for(int i = 0; i < loop; i++) {
			list.add(i);
		}
		Collections.shuffle(list);
		end = System.currentTimeMillis();
		timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Time used to shuffle " + loop + " items : " + timeInSeconds + " seconds.");
		
		begin = System.currentTimeMillis();
		for(int i : list) {
			assertEquals("" + i, new String(bigArray.get(i)));
		}
		end = System.currentTimeMillis();
		timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Time used to randomly read " + loop + " items : " + timeInSeconds + " seconds.");
	}
	
	@Test
	public void getClosestIndexTest() throws IOException {
	    bigArray = new BigArrayImpl(testDir, "get_closest_index_test");
		assertNotNull(bigArray);
		
		assertTrue(IBigArray.NOT_FOUND == bigArray.getClosestIndex(System.currentTimeMillis()));
		
		int loop = 2000000;
		long begin = System.currentTimeMillis();
		TestUtil.sleepQuietly(500);
		for(int i = 0; i < loop; i++) {
			bigArray.append(("" + i).getBytes());
		}
		TestUtil.sleepQuietly(500);
		long end = System.currentTimeMillis();
		
		long midTs = (end + begin) / 2;
		
		assertTrue(0L == bigArray.getClosestIndex(begin));
		assertTrue(loop - 1 == bigArray.getClosestIndex(end));

		long midIndex = bigArray.getClosestIndex(midTs);
		assertTrue(0L < midIndex);
		assertTrue(loop -1 > midIndex);
		
		long closestTime = bigArray.getTimestamp(midIndex);
		long closestTimeBefore = bigArray.getTimestamp(midIndex - 1);
		long closestTimeAfter = bigArray.getTimestamp(midIndex + 1);
		assertTrue(closestTimeBefore <= closestTime);
		assertTrue(closestTimeAfter >= closestTime);		
	}
	
	@After
	public void clean() throws IOException {
		if (bigArray != null) {
			bigArray.removeAll();
		}
	}

}
