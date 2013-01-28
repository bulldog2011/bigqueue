package com.leansoft.thriftqueue.load;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.leansoft.bigqueue.thrift.QueueRequest;
import com.leansoft.bigqueue.thrift.QueueResponse;
import com.leansoft.bigqueue.thrift.ResultCode;
import com.leansoft.thriftqueue.load.helper.RandomLogGenerator;

public class LoadTester {
	
	static final int INNER_LOOP = 1000;
	static final int OUTTER_LOOP = 200;
	static final int PRODUCER_THREAD_NUM = 50;
	static final int CONSUMER_THREAD_NUM = 50;
	static final int LOG_MESSAGE_SIZE_LIMIT = 4 * 1024; // this is the max size, average log message size should / 2 

	/**
	 * Main entry of the load tester
	 * 
	 * @throws TTransportException 
	 */
	public static void main(String[] args) {

		testProducing();
		
		testConsuming();
		
//		testConcurrency();
	}
	
	static void testConcurrency() {
		try {
			// prepare the test
			CountDownLatch pLatch = new CountDownLatch(PRODUCER_THREAD_NUM);
			BlockingQueue<TestResult> producerTestResultQueue = new LinkedBlockingQueue<TestResult>();
	
			// prepare the test
			CountDownLatch cLatch = new CountDownLatch(CONSUMER_THREAD_NUM);
			BlockingQueue<TestResult> consumerTestResultQueue = new LinkedBlockingQueue<TestResult>();
			
			// producer load generating
			long begin = System.currentTimeMillis();
			for(int i = 0; i < PRODUCER_THREAD_NUM; i++) {
				TestClient client = new TestClient(); // client per thread
				LogProducer logProducer = new LogProducer(pLatch, client, producerTestResultQueue);
				logProducer.start();
			}
	
			
			// consumer load consuming
			for(int i = 0; i < CONSUMER_THREAD_NUM; i++) {
				TestClient client = new TestClient(); // client per thread
				LogConsumer logConsumer = new LogConsumer(cLatch, client, consumerTestResultQueue);
				logConsumer.start();
			}
			
			
			// summarize producer test result
			TestResult sumProducerResult = new TestResult();
			for(int i = 0; i < PRODUCER_THREAD_NUM; i++) {
				TestResult testResult = producerTestResultQueue.take();
				sumProducerResult.failureCount += testResult.failureCount;
				sumProducerResult.successCount += testResult.successCount;
				sumProducerResult.exceptionCount += testResult.exceptionCount;
				sumProducerResult.totalDelay += testResult.totalDelay;
				sumProducerResult.totalBytesSent += testResult.totalBytesSent;	
			}
			
			
			// summarize consumer test result
			TestResult sumConsumerResult = new TestResult();
			for(int i = 0; i < CONSUMER_THREAD_NUM; i++) {
				TestResult testResult = consumerTestResultQueue.take();
				sumConsumerResult.failureCount += testResult.failureCount;
				sumConsumerResult.successCount += testResult.successCount;
				sumConsumerResult.exceptionCount += testResult.exceptionCount;
				sumConsumerResult.totalDelay += testResult.totalDelay;
				sumConsumerResult.totalBytesReceived += testResult.totalBytesReceived;	
			}
			
			long end = System.currentTimeMillis();
			long duration = end - begin;
			
			System.out.println("-------------------------------------------------------");
			System.out.println("Concurrency Test Report:");
			
			// producer test reporting
			System.out.println("-------------------------------------------------------");
			System.out.println("Producer Report:");
			System.out.println("-------------------------------------------------------");
			System.out.println("Log producer thread number : " + PRODUCER_THREAD_NUM);
			System.out.println("Test duration(s) : " + duration / 1000.0);
			long totalLogsSent = sumProducerResult.failureCount + sumProducerResult.successCount;
			System.out.println("Total logs sent : " + totalLogsSent);
			System.out.println("Log sending success count : " + sumProducerResult.successCount);
			System.out.println("Log sending failure count : " + sumProducerResult.failureCount);
			System.out.println("Log sending exception count : " + sumProducerResult.exceptionCount);
			System.out.println("Total byes produced : " + sumProducerResult.totalBytesSent);
			System.out.println("Average log size : " + sumProducerResult.totalBytesSent / (double)totalLogsSent);
			double throughput = sumProducerResult.totalBytesSent;
			throughput = throughput / (1024 * 1024);
			throughput = throughput / (duration / 1000.0);
			System.out.println("Throughput(MB/s) : " + throughput);
			System.out.println("Average log sending delay(ms) : " + ((double)sumProducerResult.totalDelay) / totalLogsSent);
			
			System.out.println("-------------------------------------------------------");
			System.out.println("Consumer Report:");
			System.out.println("-------------------------------------------------------");
			System.out.println("Log consumer thread number : " + CONSUMER_THREAD_NUM);
			System.out.println("Test duration(s) : " + duration / 1000.0);
			long totalLogsReceived = sumConsumerResult.failureCount + sumConsumerResult.successCount;
			System.out.println("Total logs received : " + totalLogsReceived);
			System.out.println("Log receiving success count : " + sumConsumerResult.successCount);
			System.out.println("Log receiving failure count : " + sumConsumerResult.failureCount);
			System.out.println("Log receiving exception count : " + sumConsumerResult.exceptionCount);
			System.out.println("Total byes received : " + sumConsumerResult.totalBytesReceived);
			System.out.println("Average log size : " + sumConsumerResult.totalBytesReceived / (double)totalLogsReceived);
			throughput = sumConsumerResult.totalBytesReceived;
			throughput = throughput / (1024 * 1024);
			throughput = throughput / (duration / 1000.0);
			System.out.println("Throughput(MB/s) : " + throughput);
			System.out.println("Average log receiving delay(ms) : " + ((double)sumConsumerResult.totalDelay) / totalLogsReceived);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static void testProducing() {
		try {
			// prepare the test
			CountDownLatch pLatch = new CountDownLatch(PRODUCER_THREAD_NUM);
			BlockingQueue<TestResult> testResultQueue = new LinkedBlockingQueue<TestResult>();
	
			// load testing
			long begin = System.currentTimeMillis();
			for(int i = 0; i < PRODUCER_THREAD_NUM; i++) {
				TestClient client = new TestClient(); // client per thread
				LogProducer logProducer = new LogProducer(pLatch, client, testResultQueue);
				logProducer.start();
			}
			
			// summarize test result
			TestResult sumResult = new TestResult();
			for(int i = 0; i < PRODUCER_THREAD_NUM; i++) {
				TestResult testResult = testResultQueue.take();
				sumResult.failureCount += testResult.failureCount;
				sumResult.successCount += testResult.successCount;
				sumResult.exceptionCount += testResult.exceptionCount;
				sumResult.totalDelay += testResult.totalDelay;
				sumResult.totalBytesSent += testResult.totalBytesSent;	
			}
			long end = System.currentTimeMillis();
			long duration = end - begin;
			
			// reporting
			System.out.println("-------------------------------------------------------");
			System.out.println("Producer Report:");
			System.out.println("-------------------------------------------------------");
			System.out.println("Log producer thread number : " + PRODUCER_THREAD_NUM);
			System.out.println("Test duration(s) : " + duration / 1000.0);
			long totalLogsSent = sumResult.failureCount + sumResult.successCount;
			System.out.println("Total logs sent : " + totalLogsSent);
			System.out.println("Log sending success count : " + sumResult.successCount);
			System.out.println("Log sending failure count : " + sumResult.failureCount);
			System.out.println("Log sending exception count : " + sumResult.exceptionCount);
			System.out.println("Total byes produced : " + sumResult.totalBytesSent);
			System.out.println("Average log size : " + sumResult.totalBytesSent / (double)totalLogsSent);
			double throughput = sumResult.totalBytesSent;
			throughput = throughput / (1024 * 1024);
			throughput = throughput / (duration / 1000.0);
			System.out.println("Throughput(MB/s) : " + throughput);
			System.out.println("Average log sending delay(ms) : " + ((double)sumResult.totalDelay) / totalLogsSent);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	static void testConsuming() {
		try {
			// prepare the test
			CountDownLatch cLatch = new CountDownLatch(CONSUMER_THREAD_NUM);
			BlockingQueue<TestResult> testResultQueue = new LinkedBlockingQueue<TestResult>();
	
			// load testing
			long begin = System.currentTimeMillis();
			for(int i = 0; i < CONSUMER_THREAD_NUM; i++) {
				TestClient client = new TestClient(); // client per thread
				LogConsumer logConsumer = new LogConsumer(cLatch, client, testResultQueue);
				logConsumer.start();
			}
			
			// summarize test result
			TestResult sumResult = new TestResult();
			for(int i = 0; i < CONSUMER_THREAD_NUM; i++) {
				TestResult testResult = testResultQueue.take();
				sumResult.failureCount += testResult.failureCount;
				sumResult.successCount += testResult.successCount;
				sumResult.exceptionCount += testResult.exceptionCount;
				sumResult.totalDelay += testResult.totalDelay;
				sumResult.totalBytesReceived += testResult.totalBytesReceived;	
			}
			long end = System.currentTimeMillis();
			long duration = end - begin;
			
			System.out.println("-------------------------------------------------------");
			System.out.println("Consumer Report:");
			System.out.println("-------------------------------------------------------");
			System.out.println("Log consumer thread number : " + CONSUMER_THREAD_NUM);
			System.out.println("Test duration(s) : " + duration / 1000.0);
			long totalLogsReceived = sumResult.failureCount + sumResult.successCount;
			System.out.println("Total logs received : " + totalLogsReceived);
			System.out.println("Log receiving success count : " + sumResult.successCount);
			System.out.println("Log receiving failure count : " + sumResult.failureCount);
			System.out.println("Log receiving exception count : " + sumResult.exceptionCount);
			System.out.println("Total byes received : " + sumResult.totalBytesReceived);
			System.out.println("Average log size : " + sumResult.totalBytesReceived / (double)totalLogsReceived);
			double throughput = sumResult.totalBytesReceived;
			throughput = throughput / (1024 * 1024);
			throughput = throughput / (duration / 1000.0);
			System.out.println("Throughput(MB/s) : " + throughput);
			System.out.println("Average log receiving delay(ms) : " + ((double)sumResult.totalDelay) / totalLogsReceived);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
    static class TestResult {
		int failureCount; // send or receive logs failure
		int successCount; // send or recieve logs success
		int exceptionCount; // test thread throws exception
		long totalDelay;
		long totalBytesSent;
		long totalBytesReceived;
		int loopCount;
	}
	
	
	static class LogProducer extends Thread {
		private final CountDownLatch latch;
		private final TestClient client;
		private final Queue<TestResult> queue;
		
		private byte[] logEvent;
		
		public LogProducer(CountDownLatch latch, TestClient client, Queue<TestResult> queue) throws TException {
			this.latch = latch;
			this.client = client;
			this.queue = queue;
			
			this.logEvent = RandomLogGenerator.genRandomLogEventBytes(LOG_MESSAGE_SIZE_LIMIT);
		}
		
		public void run() {
			TestResult testResult = new TestResult();
			
			try {
				latch.countDown();
				latch.await();
				
				for(int i = 0; i < OUTTER_LOOP; i++) {
					//System.out.println(new Date() + ", thread " + Thread.currentThread().getId() + " begin to send " + INNER_LOOP + " log events ...");
					for(int j = 0; j < INNER_LOOP; j++) {
						// DO NOT generate random logs here, this is accumulatively time consuming 
						// and will affact the result of performance test, 
						// instead pre-gen a log when the producer is initialized
//						byte[] logEvent = RandomLogGenerator.genRandomLogEventBytes(LOG_MESSAGE_SIZE_LIMIT);
						long begin = System.currentTimeMillis();
						QueueRequest request = new QueueRequest();
						request.setData(logEvent);
						QueueResponse response = client.enqueue(TestClient.TOPIC, request);
						long end = System.currentTimeMillis();
						if (response.getResultCode() == ResultCode.SUCCESS) {
							testResult.successCount++;
						} else {
							testResult.failureCount++;
						}
						testResult.totalBytesSent += logEvent.length;
						testResult.totalDelay += end - begin;
						testResult.loopCount++;
					}
					// Thread.sleep(2000); // simulate real scenario
				}
			
			} catch (Exception e) {
				e.printStackTrace();
				testResult.exceptionCount ++;
			} finally {
				client.close();
			}
			
			queue.offer(testResult);
			
		}
	}

	static class LogConsumer extends Thread {
		private final CountDownLatch latch;
		private final TestClient client;
		private final Queue<TestResult> queue;
		
		public LogConsumer(CountDownLatch latch, TestClient client, Queue<TestResult> queue) {
			this.latch = latch;
			this.client = client;
			this.queue = queue;
		}
		
		public void run() {
			TestResult testResult = new TestResult();
			
			try {
				latch.countDown();
				latch.await();
				
				int nullCount = 0;
				
				while(true) {
					long begin = System.currentTimeMillis();
					QueueResponse response = this.client.dequeue(TestClient.TOPIC);
					long end = System.currentTimeMillis();
					if (response.getData() != null) {
						if (response.getResultCode() == ResultCode.SUCCESS) {
							testResult.successCount++;
						} else {
							testResult.failureCount++;
						}
						nullCount = 0;
						testResult.totalBytesReceived += response.getData().length;
						testResult.totalDelay += end - begin;
						testResult.loopCount++;
					} else {
						nullCount++;
					}
					
					if (nullCount > 100) { // nothing to consume
						System.out.println("Consumer thread " + Thread.currentThread().getId() + " exit.");
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				testResult.exceptionCount++;
			} finally {
				client.close();
			}
			
			queue.offer(testResult);
		}
	}
	
}
