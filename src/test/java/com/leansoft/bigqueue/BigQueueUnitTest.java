package com.leansoft.bigqueue;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;

public class BigQueueUnitTest {
	
	private String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/unit";
	private IBigQueue bigQueue;

	@Test
	public void simpleTest() throws IOException {
		for(int i = 1; i <= 2; i++) {
		
			bigQueue = new BigQueueImpl(testDir, "simple_test");
			assertNotNull(bigQueue);
			
			for(int j = 1; j <= 3; j++) {
				assertTrue(bigQueue.size() == 0L);
				assertTrue(bigQueue.isEmpty());
				
				assertNull(bigQueue.dequeue());
				assertNull(bigQueue.peek());
	
				
				bigQueue.enqueue("hello".getBytes());
				assertTrue(bigQueue.size() == 1L);
				assertTrue(!bigQueue.isEmpty());
				assertEquals("hello", new String(bigQueue.peek()));
				assertEquals("hello", new String(bigQueue.dequeue()));
				assertNull(bigQueue.dequeue());
				
				bigQueue.enqueue("world".getBytes());
				bigQueue.flush();
				assertTrue(bigQueue.size() == 1L);
				assertTrue(!bigQueue.isEmpty());
				assertEquals("world", new String(bigQueue.dequeue()));
				assertNull(bigQueue.dequeue());
				
			}
			
			bigQueue.close();
		
		}
	}
	
	@Test
	public void bigLoopTest() throws IOException {
		bigQueue = new BigQueueImpl(testDir, "big_loop_test");
		assertNotNull(bigQueue);
		
		int loop = 10000000;
		for(int i = 0; i < loop; i++) {
			bigQueue.enqueue(("" + i).getBytes());
			assertTrue(bigQueue.size() == i + 1L);
			assertTrue(!bigQueue.isEmpty());
			byte[] data = bigQueue.peek();
			assertEquals("0", new String(data));
		}
		
		assertTrue(bigQueue.size() == loop);
		assertTrue(!bigQueue.isEmpty());
		assertEquals("0", new String(bigQueue.peek()));
		
		bigQueue.close();
		
		// create a new instance on exiting queue
		bigQueue = new BigQueueImpl(testDir, "big_loop_test");
		assertTrue(bigQueue.size() == loop);
		assertTrue(!bigQueue.isEmpty());
		
		for(int i = 0; i < loop; i++) {
			byte[] data = bigQueue.dequeue();
			assertEquals("" + i, new String(data));
			assertTrue(bigQueue.size() == loop - i - 1);
		}
		
		assertTrue(bigQueue.isEmpty());
		
		bigQueue.gc();
		
		bigQueue.close();
	}
	
	@Test
	public void loopTimingTest() throws IOException {
		bigQueue = new BigQueueImpl(testDir, "loop_timing_test");
		assertNotNull(bigQueue);
		
		int loop = 10000000;
		long begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			bigQueue.enqueue(("" + i).getBytes());
		}
		long end = System.currentTimeMillis();
		int timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Time used to enqueue " + loop + " items : " + timeInSeconds + " seconds.");
		
		begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			assertEquals("" + i, new String(bigQueue.dequeue()));
		}
		end = System.currentTimeMillis();
		timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");
	}
	
	
	@After
	public void clean() throws IOException {
		if (bigQueue != null) {
			bigQueue.removeAll();
		}
	}

}
