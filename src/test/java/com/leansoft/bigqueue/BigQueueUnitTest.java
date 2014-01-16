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
		
		int loop = 1000000;
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
		
		int loop = 1000000;
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
	
	@Test
	public void testInvalidDataPageSize() throws IOException {
		try {
			bigQueue = new BigQueueImpl(testDir, "testInvalidDataPageSize", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE - 1);
			fail("should throw invalid page size exception");
		} catch (IllegalArgumentException iae) {
			// ecpected
		}
		// ok
		bigQueue = new BigQueueImpl(testDir, "testInvalidDataPageSize", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
	}


    @Test
    public void testApplyForEachDoNotChangeTheQueue() throws Exception {
        bigQueue = new BigQueueImpl(testDir, "testApplyForEachDoNotChangeTheQueue", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
        bigQueue.enqueue("1".getBytes());
        bigQueue.enqueue("2".getBytes());
        bigQueue.enqueue("3".getBytes());

        DefaultItemIterator dii = new DefaultItemIterator();
        bigQueue.applyForEach(dii);
        System.out.println("[" + dii.getCount() + "] " + dii.toString());

        assertEquals(3, bigQueue.size());
        assertEquals(bigQueue.size(), dii.getCount());

        assertArrayEquals("1".getBytes(), bigQueue.dequeue());
        assertArrayEquals("2".getBytes(), bigQueue.dequeue());
        assertArrayEquals("3".getBytes(), bigQueue.dequeue());

        assertEquals(0, bigQueue.size());
    }

    @Test
    public void concurrentApplyForEachTest() throws Exception {
        bigQueue = new BigQueueImpl(testDir, "concurrentApplyForEachTest", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE );

        final long N = 100000;

        Thread publisher = new Thread(new Runnable() {
            private Long item = 1l;

            @Override
            public void run() {
                for (long i=0; i<N; i++)
                    try {
                        bigQueue.enqueue(item.toString().getBytes());
                        item++;
                        Thread.yield();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
        });

        Thread subscriber = new Thread(new Runnable() {
            private long item = 0l;

            @Override
            public void run() {
                for (long i=0; i<N; i++)
                    try {
                        if (bigQueue.size() > 0) {
                            byte[] bytes = bigQueue.dequeue();
                            String str = new String(bytes);
                            long curr = Long.parseLong(str);
                            assertEquals(item+1, curr);
                            item = curr;
                        }

                        Thread.yield();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }
        });

        subscriber.start();
        publisher.start();

        for (long i=0; i<N; i+=N/100) {
            DefaultItemIterator dii = new DefaultItemIterator();
            bigQueue.applyForEach(dii);
            System.out.println("[" + dii.getCount() + "] " + dii.toString());
            Thread.sleep(2);
        }

        publisher.join();
        subscriber.join();
    }

	@After
	public void clean() throws IOException {
		if (bigQueue != null) {
			bigQueue.removeAll();
		}
	}

    private class DefaultItemIterator implements IBigQueue.ItemIterator {
        private long count = 0;
        private StringBuilder sb = new StringBuilder();

        public void forEach(byte[] item) throws IOException {
            try {
                if (count<20) {
                    sb.append(new String(item));
                    sb.append(", ");

                }
                count++;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        public long getCount() {
            return count;
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
