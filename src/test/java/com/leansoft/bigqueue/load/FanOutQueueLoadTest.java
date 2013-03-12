package com.leansoft.bigqueue.load;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.leansoft.bigqueue.FanOutQueueImpl;
import com.leansoft.bigqueue.IFanOutQueue;
import com.leansoft.bigqueue.TestUtil;

public class FanOutQueueLoadTest {

	private static String testDir = TestUtil.TEST_BASE_DIR + "fanout_queue/load";
	private static IFanOutQueue foQueue;
	
	// configurable parameters
	//////////////////////////////////////////////////////////////////
	private static int loop = 5;
	private static int totalItemCount = 100000;
	private static int producerNum = 4;
	private static int consumerGroupANum = 2;
	private static int consumerGroupBNum = 4;
	private static int messageLength = 1024;
	//////////////////////////////////////////////////////////////////

	private static enum Status {
		ERROR,
		SUCCESS
	}
	
	private static class Result {
		Status status;
	}
	
	@After
	public void clean() throws IOException {
		if (foQueue != null) {
			foQueue.removeAll();
		}
	}
    
	private static class Producer extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final AtomicInteger producingItemCount;
		private final Set<String> itemSetA;
		private final Set<String> itemSetB;
		
		public Producer(CountDownLatch latch, Queue<Result> resultQueue, AtomicInteger producingItemCount, Set<String> itemSetA, Set<String> itemSetB) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.producingItemCount = producingItemCount;
			this.itemSetA = itemSetA;
			this.itemSetB = itemSetB;
		}
		
		public void run() {
			Result result = new Result();
			String rndString = TestUtil.randomString(messageLength);
			try {
				latch.countDown();
				latch.await();
				
				while(true) {
					int count = producingItemCount.incrementAndGet();
					if(count > totalItemCount) break;
					String item = rndString + count;
					itemSetA.add(item);
					itemSetB.add(item);
					foQueue.enqueue(item.getBytes());
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}	
	}
	
	private static class Consumer extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final AtomicInteger consumingItemCount;
		private final String fanoutId;
		private final Set<String> itemSet;
		
		public Consumer(CountDownLatch latch, Queue<Result> resultQueue, String fanoutId, AtomicInteger consumingItemCount, Set<String> itemSet) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			this.consumingItemCount = consumingItemCount;
			this.fanoutId = fanoutId;
			this.itemSet = itemSet;
		}
		
		public void run() {
			Result result = new Result();
			try {
				latch.countDown();
				latch.await();
				
				while(true) {
					String item = null;
					int index = consumingItemCount.getAndIncrement();
					if (index >= totalItemCount) break;
					
					byte[] data = foQueue.dequeue(fanoutId);
					while(data == null) {
						Thread.sleep(10);
						data = foQueue.dequeue(fanoutId);
					}
					item = new String(data);
					assertNotNull(item);
					assertTrue(itemSet.remove(item));
				}
				result.status = Status.SUCCESS;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}
		
	}
	
	@Test
	public void runTest() throws Exception {
		foQueue = new FanOutQueueImpl(testDir, "load_test_one");
		
		System.out.println("Load test begin ...");
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
			this.doRunProduceThenConsume();
			
			// reset
			foQueue.removeAll();
		}
		
		foQueue.close();
		foQueue = new FanOutQueueImpl(testDir, "load_test_two");
		
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
			this.doRunMixed();
			
			// reset
			// reset
			foQueue.removeAll();
		}
		System.out.println("Load test finished successfully.");
	}
	
	public void doRunMixed() throws Exception {
		final AtomicInteger producerItemCount = new AtomicInteger(0);
		final AtomicInteger consumerGroupAItemCount = new AtomicInteger(0);
		final AtomicInteger consumerGroupBItemCount = new AtomicInteger(0);
		
	    final Set<String> itemSetA = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
	    final Set<String> itemSetB = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
		
		String consumerGroupAFanoutId = "groupA";
		String consumerGroupBFanoutId = "groupB";
		
		CountDownLatch platch = new CountDownLatch(producerNum);
		CountDownLatch clatchGroupA = new CountDownLatch(consumerGroupANum);
		CountDownLatch clatchGroupB = new CountDownLatch(consumerGroupBNum);
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerGroupAResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerGroupBResults = new LinkedBlockingQueue<Result>();
		
		//run testing
		for(int i = 0; i < producerNum; i++) {
			Producer p = new Producer(platch, producerResults, producerItemCount, itemSetA, itemSetB);
			p.start();
		}
		
		for(int i = 0; i < consumerGroupANum; i++) {
			Consumer c = new Consumer(clatchGroupA, consumerGroupAResults, consumerGroupAFanoutId, consumerGroupAItemCount, itemSetA);
			c.start();
		}
		
		for(int i = 0; i < consumerGroupBNum; i++) {
			Consumer c = new Consumer(clatchGroupB, consumerGroupBResults, consumerGroupBFanoutId, consumerGroupBItemCount, itemSetB);
			c.start();
		}
		
		for(int i = 0; i < consumerGroupANum; i++) {
			Result result = consumerGroupAResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		
		for(int i = 0; i < consumerGroupBNum; i++) {
			Result result = consumerGroupBResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		for(int i = 0; i < producerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		
		assertTrue(itemSetA.isEmpty());
		assertTrue(foQueue.isEmpty(consumerGroupAFanoutId));
		assertTrue(foQueue.size(consumerGroupAFanoutId) == 0);
		
		assertTrue(itemSetB.isEmpty());
		assertTrue(foQueue.isEmpty(consumerGroupBFanoutId));
		assertTrue(foQueue.size(consumerGroupBFanoutId) == 0);
		
		assertTrue(!foQueue.isEmpty());
		assertTrue(foQueue.size() == totalItemCount);
	}
	
	public void doRunProduceThenConsume() throws Exception {
		//prepare
		
		final AtomicInteger producerItemCount = new AtomicInteger(0);
		final AtomicInteger consumerGroupAItemCount = new AtomicInteger(0);
		final AtomicInteger consumerGroupBItemCount = new AtomicInteger(0);
		
	    final Set<String> itemSetA = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
	    final Set<String> itemSetB = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
		
		String consumerGroupAFanoutId = "groupA";
		String consumerGroupBFanoutId = "groupB";
		
		CountDownLatch platch = new CountDownLatch(producerNum);
		CountDownLatch clatchGroupA = new CountDownLatch(consumerGroupANum);
		CountDownLatch clatchGroupB = new CountDownLatch(consumerGroupBNum);
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerGroupAResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerGroupBResults = new LinkedBlockingQueue<Result>();
		
		//run testing
		for(int i = 0; i < producerNum; i++) {
			Producer p = new Producer(platch, producerResults, producerItemCount, itemSetA, itemSetB);
			p.start();
		}
		
		for(int i = 0; i < producerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		assertTrue(!foQueue.isEmpty());
		assertTrue(foQueue.size() == totalItemCount);
		foQueue.flush();
		
		assertTrue(itemSetA.size() == totalItemCount);
		assertTrue(itemSetB.size() == totalItemCount);
		
		for(int i = 0; i < consumerGroupANum; i++) {
			Consumer c = new Consumer(clatchGroupA, consumerGroupAResults, consumerGroupAFanoutId, consumerGroupAItemCount, itemSetA);
			c.start();
		}
		
		for(int i = 0; i < consumerGroupBNum; i++) {
			Consumer c = new Consumer(clatchGroupB, consumerGroupBResults, consumerGroupBFanoutId, consumerGroupBItemCount, itemSetB);
			c.start();
		}
		
		for(int i = 0; i < consumerGroupANum; i++) {
			Result result = consumerGroupAResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		
		for(int i = 0; i < consumerGroupBNum; i++) {
			Result result = consumerGroupBResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		assertTrue(itemSetA.isEmpty());
		assertTrue(foQueue.isEmpty(consumerGroupAFanoutId));
		assertTrue(foQueue.size(consumerGroupAFanoutId) == 0);
		
		assertTrue(itemSetB.isEmpty());
		assertTrue(foQueue.isEmpty(consumerGroupBFanoutId));
		assertTrue(foQueue.size(consumerGroupBFanoutId) == 0);
		
		assertTrue(!foQueue.isEmpty());
		assertTrue(foQueue.size() == totalItemCount);
	}

}
