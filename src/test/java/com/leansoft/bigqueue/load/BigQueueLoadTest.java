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

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import com.leansoft.bigqueue.TestUtil;

public class BigQueueLoadTest {
	
	private static String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/load";
	private static IBigQueue bigQueue;
	
	// configurable parameters
	//////////////////////////////////////////////////////////////////
	private static int loop = 5;
	private static int totalItemCount = 100000;
	private static int producerNum = 4;
	private static int consumerNum = 4;
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
		if (bigQueue != null) {
			bigQueue.removeAll();
		}
	}
	
	private static final AtomicInteger producingItemCount = new AtomicInteger(0);
	private static final AtomicInteger consumingItemCount = new AtomicInteger(0);
    private static final Set<String> itemSet = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
    
	private static class Producer extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		
		public Producer(CountDownLatch latch, Queue<Result> resultQueue) {
			this.latch = latch;
			this.resultQueue = resultQueue;
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
					itemSet.add(item);
					bigQueue.enqueue(item.getBytes());
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
		
		public Consumer(CountDownLatch latch, Queue<Result> resultQueue) {
			this.latch = latch;
			this.resultQueue = resultQueue;
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
					
					byte[] data = bigQueue.dequeue();
					while(data == null) {
						Thread.sleep(10);
						data = bigQueue.dequeue();
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
		bigQueue = new BigQueueImpl(testDir, "load_test");
		
		System.out.println("Load test begin ...");
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
			this.doRunProduceThenConsume();
			
			// reset
			producingItemCount.set(0);
			consumingItemCount.set(0);
			bigQueue.gc();
		}
		
		bigQueue.close();
		bigQueue = new BigQueueImpl(testDir, "load_test");
		
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
			this.doRunMixed();
			
			// reset
			producingItemCount.set(0);
			consumingItemCount.set(0);
			bigQueue.gc();
		}
		System.out.println("Load test finished successfully.");
	}
	
	public void doRunProduceThenConsume() throws Exception {
		//prepare
		CountDownLatch platch = new CountDownLatch(producerNum);
		CountDownLatch clatch = new CountDownLatch(consumerNum);
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();
		
		//run testing
		for(int i = 0; i < producerNum; i++) {
			Producer p = new Producer(platch, producerResults);
			p.start();
		}
		
		for(int i = 0; i < producerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		assertTrue(!bigQueue.isEmpty());
		assertTrue(bigQueue.size() == totalItemCount);
		bigQueue.flush();
		
		assertTrue(itemSet.size() == totalItemCount);
		
		for(int i = 0; i < consumerNum; i++) {
			Consumer c = new Consumer(clatch, consumerResults);
			c.start();
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		assertTrue(itemSet.isEmpty());
		assertTrue(bigQueue.isEmpty());
		assertTrue(bigQueue.size() == 0);
	}
	

	public void doRunMixed() throws Exception {
		//prepare
		CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();
		
		//run testing
		for(int i = 0; i < producerNum; i++) {
			Producer p = new Producer(allLatch, producerResults);
			p.start();
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Consumer c = new Consumer(allLatch, consumerResults);
			c.start();
		}
		
		//verify
		for(int i = 0; i < producerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
		}
		
		assertTrue(itemSet.isEmpty());
		assertTrue(bigQueue.isEmpty());
		assertTrue(bigQueue.size() == 0);
	}

}
