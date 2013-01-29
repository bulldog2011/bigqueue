package com.leansoft.bigqueue.perf;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import com.leansoft.bigqueue.TestUtil;

public class BigQueuePerfTest {
	
	private static String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/perf";
	private static IBigQueue bigQueue;
	private static BlockingQueue<byte[]> memoryQueue = new LinkedBlockingQueue<byte[]>();
	
	static {
		try {
			bigQueue = new BigQueueImpl(testDir, "perf_test");
		} catch (IOException e) {
			fail("fail to init big queue");
		}
	}
	
	// configurable parameters
	//////////////////////////////////////////////////////////////////
	private static int loop = 5;
	private static int totalItemCount = 1000000;
	private static int producerNum = 2;
	private static int consumerNum = 2;
	private static int messageLength = 1024;
	private static TestType testType = TestType.BIG_QUEUE_TEST;
	//////////////////////////////////////////////////////////////////
	
	private static enum TestType {
		IN_MEMORY_QUEUE_TEST,
		BIG_QUEUE_TEST
	}

	private static enum Status {
		ERROR,
		SUCCESS
	}
	
	private static class Result {
		Status status;
		long duration;
	}
	
	@After
	public void clean() throws IOException {
		if (bigQueue != null) {
			bigQueue.removeAll();
		}
	}
	
	private static final AtomicInteger producingItemCount = new AtomicInteger(0);
	private static final AtomicInteger consumingItemCount = new AtomicInteger(0);
    
	private static class Producer extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private byte[] rndBytes = TestUtil.randomString(messageLength).getBytes();
		
		public Producer(CountDownLatch latch, Queue<Result> resultQueue) {
			this.latch = latch;
			this.resultQueue = resultQueue;
		}
		
		public void run() {
			Result result = new Result();
			try {
				latch.countDown();
				latch.await();
				
				long start = System.currentTimeMillis();
				while(true) {
					int count = producingItemCount.incrementAndGet();
					if(count > totalItemCount) break;
					if (testType == TestType.IN_MEMORY_QUEUE_TEST) {
						memoryQueue.add(rndBytes);
					} else {
						bigQueue.enqueue(rndBytes);
					}
				}
				long end = System.currentTimeMillis();
				result.status = Status.SUCCESS;
				result.duration = end - start;
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
				
				long start = System.currentTimeMillis();
				while(true) {
					byte[] item = null;
					int index = consumingItemCount.getAndIncrement();
					if (index >= totalItemCount) break;
					if (testType == TestType.IN_MEMORY_QUEUE_TEST) {
						item = memoryQueue.take();
					} else {
						item = bigQueue.dequeue();
						while(item == null) {
							item = bigQueue.dequeue();
						}
					}
				}
				long end = System.currentTimeMillis();
				result.status = Status.SUCCESS;
				result.duration = end - start;
			} catch (Exception e) {
				e.printStackTrace();
				result.status = Status.ERROR;
			}
			resultQueue.offer(result);
		}
		
	}
	
	@Test
	public void runTest() throws Exception {
		System.out.println("Performance test begin ...");
		
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
			this.doRunProduceThenConsume();
			
			// reset
			producingItemCount.set(0);
			consumingItemCount.set(0);
		}
		
		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
			this.doRunMixed();
			
			// reset
			producingItemCount.set(0);
			consumingItemCount.set(0);
		}
		
		System.out.println("Performance test finished successfully.");
	}
	
	public void doRunProduceThenConsume() throws Exception {
		//prepare
		CountDownLatch platch = new CountDownLatch(producerNum);
		CountDownLatch clatch = new CountDownLatch(consumerNum);
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();
		
		long totalProducingTime = 0;
		long totalConsumingTime = 0;
		
		long start = System.currentTimeMillis();
		//run testing
		for(int i = 0; i < producerNum; i++) {
			Producer p = new Producer(platch, producerResults);
			p.start();
		}
		
		for(int i = 0; i < producerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
			totalProducingTime += result.duration;
		}
		long end = System.currentTimeMillis();
		
		if (testType == TestType.BIG_QUEUE_TEST) {
			assertTrue(!bigQueue.isEmpty());
		}
		
		System.out.println("-----------------------------------------------");
		System.out.println("Test type = " + testType);
		System.out.println("-----------------------------------------------");
		
		System.out.println("Producing test result:");
		System.out.println("Total test time = " + (end - start) + " ms.");
		System.out.println("Total item count = " + totalItemCount);
		System.out.println("Producer thread number = " + producerNum);
		System.out.println("Item message length = " + messageLength + " bytes");
		System.out.println("Total producing time = " + totalProducingTime + " ms.");
		System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
		System.out.println("-----------------------------------------------");
		
		
		start = System.currentTimeMillis();
		for(int i = 0; i < consumerNum; i++) {
			Consumer c = new Consumer(clatch, consumerResults);
			c.start();
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
			totalConsumingTime += result.duration;
		}
		end = System.currentTimeMillis();
		
		assertTrue(memoryQueue.isEmpty());
		assertTrue(bigQueue.isEmpty());
		
		System.out.println("Consuming test result:");
		System.out.println("Total test time = " + (end - start) + " ms.");
		System.out.println("Total item count = " + totalItemCount);
		System.out.println("Consumer thread number = " + consumerNum);
		System.out.println("Item message length = " + messageLength + " bytes");
		System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
		System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
		System.out.println("-----------------------------------------------");
	}
	

	public void doRunMixed() throws Exception {
		//prepare
		CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
		BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
		BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();
		
		long totalProducingTime = 0;
		long totalConsumingTime = 0;
		
		long start = System.currentTimeMillis();
		//run testing
		for(int i = 0; i < producerNum; i++) {
			Producer p = new Producer(allLatch, producerResults);
			p.start();
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Consumer c = new Consumer(allLatch, consumerResults);
			c.start();
		}
		
		//verify and report
		for(int i = 0; i < producerNum; i++) {
			Result result = producerResults.take();
			assertEquals(result.status, Status.SUCCESS);
			totalProducingTime += result.duration;
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
			totalConsumingTime += result.duration;
		}
		
		long end = System.currentTimeMillis();
		
		assertTrue(memoryQueue.isEmpty());
		assertTrue(bigQueue.isEmpty());
		
		System.out.println("-----------------------------------------------");
		System.out.println("Test type = " + testType);
		System.out.println("-----------------------------------------------");
		
		System.out.println("Total item count = " + totalItemCount);
		System.out.println("Producer thread number = " + producerNum);
		System.out.println("Consumer thread number = " + consumerNum);
		System.out.println("Item message length = " + messageLength + " bytes");
		System.out.println("Total test time = " + (end - start) + " ms.");
		System.out.println("Test type = " + testType);
		System.out.println("Total producing time = " + totalProducingTime + " ms.");
		System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
		System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
		System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
		System.out.println("-----------------------------------------------");
	}

}
