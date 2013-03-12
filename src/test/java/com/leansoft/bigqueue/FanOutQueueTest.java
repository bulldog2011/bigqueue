package com.leansoft.bigqueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;

public class FanOutQueueTest {
	
	private String testDir = TestUtil.TEST_BASE_DIR + "foqueue/unit";
	private IFanOutQueue foQueue;

	@Test
	public void simpleTest() throws IOException {
		for(int i = 1; i <= 2; i++) {
		
			foQueue = new FanOutQueueImpl(testDir, "simple_test");
			assertNotNull(foQueue);
			
			String fid = "simpleTest";
			for(int j = 1; j <= 3; j++) {
				assertTrue(foQueue.size(fid) == 0L);
				assertTrue(foQueue.isEmpty(fid));
				
				assertNull(foQueue.dequeue(fid));
				assertNull(foQueue.peek(fid));
	
				
				foQueue.enqueue("hello".getBytes());
				assertTrue(foQueue.size(fid) == 1L);
				assertTrue(!foQueue.isEmpty(fid));
				assertEquals("hello", new String(foQueue.peek(fid)));
				assertEquals("hello", new String(foQueue.dequeue(fid)));
				assertNull(foQueue.dequeue(fid));
				
				foQueue.enqueue("world".getBytes());
				foQueue.flush();
				assertTrue(foQueue.size(fid) == 1L);
				assertTrue(!foQueue.isEmpty(fid));
				assertEquals("world", new String(foQueue.dequeue(fid)));
				assertNull(foQueue.dequeue(fid));
				
			}
			
			foQueue.close();
		}
	}
	
	@Test
	public void bigLoopTest() throws IOException {
		foQueue = new FanOutQueueImpl(testDir, "big_loop_test");
		assertNotNull(foQueue);
		
		int loop = 1000000;
		String fid1 = "bigLoopTest1";
		long ts = -1;
		for(int i = 0; i < loop; i++) {
			foQueue.enqueue(("" + i).getBytes());
			assertTrue(foQueue.size(fid1) == i + 1L);
			assertTrue(!foQueue.isEmpty(fid1));
			byte[] data = foQueue.peek(fid1);
			assertEquals("0", new String(data));
			int length = foQueue.peekLength(fid1);
			assertEquals(1, length);
			if (ts == -1) {
				ts = foQueue.peekTimestamp(fid1);
			} else {
				assertTrue(ts == foQueue.peekTimestamp(fid1));
			}
		}
		
		assertTrue(foQueue.size(fid1) == loop);
		assertTrue(!foQueue.isEmpty(fid1));
		assertEquals("0", new String(foQueue.peek(fid1)));
		
		foQueue.close();
		
		// create a new instance on exiting queue
		foQueue = new FanOutQueueImpl(testDir, "big_loop_test");
		assertTrue(foQueue.size(fid1) == loop);
		assertTrue(!foQueue.isEmpty(fid1));
		
		for(int i = 0; i < loop; i++) {
			byte[] data = foQueue.dequeue(fid1);
			assertEquals("" + i, new String(data));
			assertTrue(foQueue.size(fid1) == loop - i - 1);
		}
		
		assertTrue(foQueue.isEmpty(fid1));
	
		// fan out test
		String fid2 = "bigLoopTest2";
		assertTrue(foQueue.size(fid2) == loop);
		assertTrue(!foQueue.isEmpty(fid2));
		assertEquals("0", new String(foQueue.peek(fid2)));
		
		for(int i = 0; i < loop; i++) {
			byte[] data = foQueue.dequeue(fid2);
			assertEquals("" + i, new String(data));
			assertTrue(foQueue.size(fid2) == loop - i - 1);
		}
		
		assertTrue(foQueue.isEmpty(fid2));
		
		foQueue.close();
	}
	
	@Test
	public void loopTimingTest() throws IOException {
		foQueue = new FanOutQueueImpl(testDir, "loop_timing_test");
		assertNotNull(foQueue);
		
		String fid1 = "loopTimingTest1";
		int loop = 10000000;
		long begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			foQueue.enqueue(("" + i).getBytes());
		}
		long end = System.currentTimeMillis();
		int timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Time used to enqueue " + loop + " items : " + timeInSeconds + " seconds.");
		
		begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			assertEquals("" + i, new String(foQueue.dequeue(fid1)));
		}
		end = System.currentTimeMillis();
		timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Fanout test 1, Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");
		
		String fid2 = "loopTimingTest2";
		begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			assertEquals("" + i, new String(foQueue.dequeue(fid2)));
		}
		end = System.currentTimeMillis();
		timeInSeconds = (int) ((end - begin) / 1000L);
		System.out.println("Fanout test 2, Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");
	}
	
	@Test
	public void invalidDataPageSizeTest() throws IOException {
		try {
			foQueue = new FanOutQueueImpl(testDir, "testInvalidDataPageSize", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE - 1);
			fail("should throw invalid page size exception");
		} catch (IllegalArgumentException iae) {
			// ecpected
		}
		// ok
		foQueue = new FanOutQueueImpl(testDir, "testInvalidDataPageSize", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
	}
	
	
	@Test
	public void resetQueueFrontIndexTest() throws IOException {
		foQueue = new FanOutQueueImpl(testDir, "reset_queue_front_index");
		assertNotNull(foQueue);
		
		String fid = "resetQueueFrontIndex";
		int loop = 100000;
		long begin = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			foQueue.enqueue(("" + i).getBytes());
		}
		
		assertEquals("0", new String(foQueue.peek(fid)));
		
		foQueue.resetQueueFrontIndex(fid, 1L);
		assertEquals("1", new String(foQueue.peek(fid)));
		
		foQueue.resetQueueFrontIndex(fid, 1234L);
		assertEquals("1234", new String(foQueue.peek(fid)));
		
		foQueue.resetQueueFrontIndex(fid, loop - 1);
		assertEquals((loop - 1) + "", new String(foQueue.peek(fid)));
		
		foQueue.resetQueueFrontIndex(fid, loop);
		assertNull(foQueue.peek(fid));
		
		try {
			foQueue.resetQueueFrontIndex(fid, loop + 1);
			fail("should throw IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException e) {
			// expeced
		}
	}
	
	@Test
	public void removeBeforeTest() throws IOException {
		foQueue = new FanOutQueueImpl(testDir, "remove_before", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
		
		String randomString1 = TestUtil.randomString(32);
		for(int i = 0; i < 1024 * 1024; i++) {
			foQueue.enqueue(randomString1.getBytes());
		}
		
		String fid = "removeBeforeTest";
		assertTrue(foQueue.size(fid) == 1024 * 1024);
		
		long timestamp = System.currentTimeMillis();
		String randomString2 = TestUtil.randomString(32);
		for(int i = 0; i < 1024 * 1024; i++) {
			foQueue.enqueue(randomString2.getBytes());
		}
		
		foQueue.removeBefore(timestamp);
		
		assertTrue(foQueue.size(fid) == 1024 * 1024);
		assertEquals(randomString2, new String(foQueue.peek(fid)));
	}
	
	
	@After
	public void clean() throws IOException {
		if (foQueue != null) {
			foQueue.removeAll();
		}
	}

}
