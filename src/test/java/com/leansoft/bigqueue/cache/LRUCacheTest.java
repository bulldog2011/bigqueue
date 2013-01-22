package com.leansoft.bigqueue.cache;

import static org.junit.Assert.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;

import com.leansoft.bigqueue.TestUtil;
import com.leansoft.bigqueue.cache.ILRUCache;
import com.leansoft.bigqueue.cache.LRUCacheImpl;

public class LRUCacheTest {
	
	@Test
	public void singleThreadTest() {
		
		ILRUCache<Integer, TestObject> cache = new LRUCacheImpl<Integer, TestObject>();
		
		TestObject obj = new TestObject();
		cache.put(1, obj, 500);
		
		TestObject obj1 = cache.get(1);
		assertEquals(obj, obj1);
		assertEquals(obj, obj1);
		assertFalse(obj1.isClosed());
	
		TestUtil.sleepQuietly(1000); // let 1 expire
		cache.put(2, new TestObject()); // trigger mark and sweep
		obj1 = cache.get(1);
		assertNotNull(obj1); // will not expire since there is reference count
		assertEquals(obj, obj1);
		assertFalse(obj1.isClosed());
		
		cache.release(1); // release first put
		cache.release(1); // release first get
		TestUtil.sleepQuietly(1000); // let 1 expire
		cache.put(3, new TestObject()); // trigger mark and sweep
		obj1 = cache.get(1);
		assertNotNull(obj1); // will not expire since there is reference count
		assertEquals(obj, obj1);
		assertFalse(obj1.isClosed());
		
		cache.release(1); // release second get
		cache.release(1); // release third get
		TestUtil.sleepQuietly(1000); // let 1 expire
		TestObject testObj = new TestObject();
		cache.put(4, testObj); // trigger mark and sweep
		obj1 = cache.get(1);
		assertNull(obj1);
		
		TestUtil.sleepQuietly(1000); // let the cleaner do the job
		assertTrue(obj.isClosed());
		
		assertTrue(cache.size() == 3);
		
		try {
			TestObject testObj2 = cache.remove(2);
			assertNotNull(testObj2);
			assertTrue(testObj2.closed);
		} catch (IOException e1) {
			fail("Got IOException when removing test object 2 from the cache.");
		}
		
		assertTrue(cache.size() == 2);
		
		try {
			TestObject testObj3 = cache.remove(3);
			assertNotNull(testObj3);
			assertTrue(testObj3.closed);
		} catch (IOException e1) {
			fail("Got IOException when removing test object 3 from the cache.");
		}
		
		assertTrue(cache.size() == 1);
		
		try {
			cache.removeAll();
		} catch (IOException e) {
			fail("Got IOException when closing the cache.");
		}
		
		TestUtil.sleepQuietly(1000); // let the cleaner do the job
		assertTrue(testObj.isClosed());
		
		assertTrue(cache.size() == 0);
	}
	
	
	@Test
	public void multiThreadsTest() {
		ILRUCache<Integer, TestObject> cache = new LRUCacheImpl<Integer, TestObject>();
		int threadNum = 100;
		
		Worker[] workers = new Worker[threadNum];
		
		// initialization
		for(int i = 0; i < threadNum; i++) {
			workers[i] = new Worker(i, cache);
		}
		
		// run
		for(int i = 0; i < threadNum; i++) {
			workers[i].start();
		}
		
		// wait to finish
		for(int i = 0; i < threadNum; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		assertTrue(cache.size() == 0);
		
		cache = new LRUCacheImpl<Integer, TestObject>();
		threadNum = 100;
		
		RandomWorker[] randomWorkers = new RandomWorker[threadNum];
		TestObject[] testObjs = new TestObject[threadNum];
		
		// initialization
		for(int i = 0; i < threadNum; i++) {
			testObjs[i] = new TestObject();
			cache.put(i, testObjs[i], 2000);
		}
		for(int i = 0; i < threadNum; i++) {
			randomWorkers[i] = new RandomWorker(threadNum, cache);
		}
		
		// run
		for(int i = 0; i < threadNum; i++) {
			randomWorkers[i].start();
		}
		
		// wait to finish
		for(int i = 0; i < threadNum; i++) {
			try {
				randomWorkers[i].join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		// verification
		for(int i = 0; i < threadNum; i++) {
			TestObject testObj = cache.get(i);
			assertNotNull(testObj);
			cache.release(i);
		}
		for(int i = 0; i < threadNum; i++) {
			assertFalse(testObjs[i].isClosed());
		}
		
		for(int i = 0; i < threadNum; i++) {
			cache.release(i); // release put
		}
		
		TestUtil.sleepQuietly(1000); // let the test objects expire but not expired
		cache.put(threadNum + 1, new TestObject()); // trigger mark and sweep
		
		for(int i = 0; i < threadNum; i++) {
			TestObject testObj = cache.get(i);
			assertNotNull(testObj); // hasn't expire yet
			cache.release(i);
		}
		
		TestUtil.sleepQuietly(2010); // let the test objects expire and be expired
	    TestObject tmpObj = new TestObject();
		cache.put(threadNum + 1, tmpObj); // trigger mark and sweep
		
		for(int i = 0; i < threadNum; i++) {
			TestObject testObj = cache.get(i);
			assertNull(testObj);
		}
		
		TestUtil.sleepQuietly(1000); // let the cleaner do the job
		for(int i = 0; i < threadNum; i++) {
			assertTrue(testObjs[i].isClosed());
		}
		
		assertTrue(cache.size() == 1);
		
		assertFalse(tmpObj.isClosed());
		
		try {
			cache.removeAll();
		} catch (IOException e) {
			fail("Got IOException when closing the cache");
		}
		TestUtil.sleepQuietly(1000);
		assertTrue(tmpObj.isClosed());
		
		assertTrue(cache.size() == 0);
	}
	
	private static class Worker extends Thread {
		private int id;
		private ILRUCache<Integer, TestObject> cache;
		
		public Worker(int id, ILRUCache<Integer, TestObject> cache) {
			this.id = id;
			this.cache = cache;
		}
		
		public void run() {
			TestObject testObj = new TestObject();
			cache.put(id, testObj, 500);
			
			TestObject testObj2 = cache.get(id);
			assertEquals(testObj, testObj2);
			assertEquals(testObj, testObj2);
			assertFalse(testObj2.isClosed());
			
			cache.release(id); // release first put
			cache.release(id); // release first get
			
			TestUtil.sleepQuietly(1000);
			cache.put(id + 1000, new TestObject(), 500); // trigger mark&sweep
			
			TestObject testObj3 = cache.get(id);
			assertNull(testObj3);
			TestUtil.sleepQuietly(1000); // let the cleaner do the job
			assertTrue(testObj.isClosed());
			
			
			cache.release(id + 1000);
			TestUtil.sleepQuietly(1000);
			cache.put(id + 2000, new TestObject()); // trigger mark&sweep
			
			try {
				TestObject testObj_id2000 = cache.remove(id + 2000);
				assertNotNull(testObj_id2000);
				assertTrue(testObj_id2000.isClosed());
			} catch (IOException e) {
				fail("Got IOException when removing test object id 2000 from the cache.");
			}
			
			TestObject testObj4 = cache.get(id + 1000);
			TestUtil.sleepQuietly(1000); // let the cleaner do the job
			assertNull(testObj4);
		}
	}
	
	private static class RandomWorker extends Thread {
		private int idLimit;
		private Random random = new Random();
		private ILRUCache<Integer, TestObject> cache;
		
		public RandomWorker(int idLimit, ILRUCache<Integer, TestObject> cache) {
			this.idLimit = idLimit;
			this.cache = cache;
		}
		
		public void run() {
			for(int i = 0; i < 10; i++) {
				int id = random.nextInt(idLimit);
				
				TestObject testObj = cache.get(id);
				assertNotNull(testObj);
				cache.put(id + 1000, new TestObject(), 1000);
				cache.put(id + 2000, new TestObject(), 1000);
				cache.put(id + 3000, new TestObject(), 1000);
				cache.release(id + 1000);
				cache.release(id + 3000);
				cache.release(id);
				try {
					TestObject testObj_id2000 = cache.remove(id + 2000);
					if (testObj_id2000 != null) { // maybe already removed by other threads
						assertTrue(testObj_id2000.isClosed());
					}
				} catch (IOException e) {
					fail("Got IOException when removing test object id 2000 from the cache.");
				}
			}
		}
	}
	
	private static class TestObject implements Closeable {
		
		private volatile boolean closed = false;

		public void close() throws IOException {
			closed = true;
		}
		
		public boolean isClosed() {
			return closed;
		}
		
	}

}
