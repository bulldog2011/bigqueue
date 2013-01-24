package com.leansoft.bigqueue.perf;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;
import com.leansoft.bigqueue.TestUtil;

public class BigArrayPerfTest {
	
	private static String testDir = TestUtil.TEST_BASE_DIR + "bigarray/perf";
	private static IBigArray bigArray;
	
	static {
		try {
			bigArray = new BigArrayImpl(testDir, "perf_test");
		} catch (IOException e) {
			fail("fail to init big array");
		}
	}
	
	// configurable parameters
	//////////////////////////////////////////////////////////////////
	private static int loop = 5;
	private static int totalItemCount = 1000000;
	private static int producerNum = 1;
	private static int consumerNum = 1;
	private static int messageLength = 1024;
	//////////////////////////////////////////////////////////////////

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
		if (bigArray != null) {
			bigArray.removeAll();
		}
	}
	
	private static final AtomicInteger producingItemCount = new AtomicInteger(0);
    
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
					bigArray.append(rndBytes);
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
	
	// random consumer can only work after producer
	private static class RandomConsumer extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		private final List<Long> indexList = new ArrayList<Long>();
		
		public RandomConsumer(CountDownLatch latch, Queue<Result> resultQueue) {
			this.latch = latch;
			this.resultQueue = resultQueue;
			// permute the index to let consumers consume randomly.
			for(long i = 0; i < totalItemCount; i++) {
				indexList.add(i);
			}
			Collections.shuffle(indexList);
		}
		
		public void run() {
			Result result = new Result();
			try {
				latch.countDown();
				latch.await();
				
				long start = System.currentTimeMillis();
				for(long index : indexList) {
					@SuppressWarnings("unused")
					byte[] data = bigArray.get(index);
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
	
	// sequential consumer can only work concurrently with producer
	private static class SequentialConsumer extends Thread {
		private final CountDownLatch latch;
		private final Queue<Result> resultQueue;
		
		public SequentialConsumer(CountDownLatch latch, Queue<Result> resultQueue) {
			this.latch = latch;
			this.resultQueue = resultQueue;
		}
		
		public void run() {
			Result result = new Result();
			try {
				latch.countDown();
				latch.await();
				
				long start = System.currentTimeMillis();
				for(long index = 0; index < totalItemCount; index++) {
					while(index >= bigArray.getHeadIndex()) {
						Thread.sleep(20); // no item to consume yet, just wait a moment
					}
					@SuppressWarnings("unused")
					byte[] data = bigArray.get(index);
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
			bigArray.removeAll();
		}

		for(int i = 0; i < loop; i++) {
			System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
			this.doRunMixed();
			
			// reset
			producingItemCount.set(0);
			bigArray.removeAll();
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
		
		assertTrue(bigArray.size() == totalItemCount);
		
		System.out.println("-----------------------------------------------");
		
		System.out.println("Producing test result:");
		System.out.println("Total test time = " + (end - start) + " ms.");
		System.out.println("Total items produced = " + totalItemCount);
		System.out.println("Producer thread number = " + producerNum);
		System.out.println("Item message length = " + messageLength + " bytes");
		System.out.println("Total producing time = " + totalProducingTime + " ms.");
		System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
		System.out.println("-----------------------------------------------");
		
		start = System.currentTimeMillis();
		for(int i = 0; i < consumerNum; i++) {
			RandomConsumer c = new RandomConsumer(clatch, consumerResults);
			c.start();
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
			totalConsumingTime += result.duration;
		}
		end = System.currentTimeMillis();
		
		System.out.println("-----------------------------------------------");
		System.out.println("Random consuming test result:");
		System.out.println("Total test time = " + (end - start) + " ms.");
		System.out.println("Total items consumed = " + totalItemCount);
		System.out.println("Consumer thread number = " + consumerNum);
		System.out.println("Item message length = " + messageLength + " bytes");
		System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
		System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
		System.out.println("-----------------------------------------------");
		
		start = System.currentTimeMillis();
		for(int i = 0; i < consumerNum; i++) {
			SequentialConsumer c = new SequentialConsumer(clatch, consumerResults);
			c.start();
		}
		
		for(int i = 0; i < consumerNum; i++) {
			Result result = consumerResults.take();
			assertEquals(result.status, Status.SUCCESS);
			totalConsumingTime += result.duration;
		}
		end = System.currentTimeMillis();
		
		System.out.println("-----------------------------------------------");
		System.out.println("Sequential consuming test result:");
		System.out.println("Total test time = " + (end - start) + " ms.");
		System.out.println("Total items consumed = " + totalItemCount);
		System.out.println("Consumer thread number = " + consumerNum);
		System.out.println("Item message length = " + messageLength + " bytes");
		System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
		System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
		System.out.println("-----------------------------------------------");
		
		assertTrue(bigArray.size() == totalItemCount);
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
			SequentialConsumer c = new SequentialConsumer(allLatch, consumerResults);
			c.start();
		}
		
		//verify
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
		
		assertTrue(bigArray.size() == totalItemCount);
		
		System.out.println("-----------------------------------------------");
		
		System.out.println("Total item count = " + totalItemCount);
		System.out.println("Producer thread number = " + producerNum);
		System.out.println("Consumer thread number = " + consumerNum);
		System.out.println("Item message length = " + messageLength + " bytes");
		System.out.println("Total test time = " + (end - start) + " ms.");
		System.out.println("Total producing time = " + totalProducingTime + " ms.");
		System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
		System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
		System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
		System.out.println("-----------------------------------------------");
	}

}
