package com.leansoft.bigqueue.page;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Test;

import com.leansoft.bigqueue.TestUtil;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;
import com.leansoft.bigqueue.utils.FileUtil;

public class MappedPageTest {
	
	private IMappedPageFactory mappedPageFactory;
	private String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/unit/mapped_page_test";

	@Test
	public void testSingleThread() throws IOException {
		int pageSize = 1024 * 1024 * 32;
		mappedPageFactory = new MappedPageFactoryImpl(pageSize, testDir + "/test_single_thread", 2 * 1000);
		
		IMappedPage mappedPage = this.mappedPageFactory.acquirePage(0);
		assertNotNull(mappedPage);
		
		ByteBuffer buffer = mappedPage.getLocal(0);
		assertTrue(buffer.limit() == pageSize);
		assertTrue(buffer.position() == 0);
		
		
		for(int i = 0; i < 10000; i++) {
			String hello = "hello world";
			int length = hello.getBytes().length;
			mappedPage.getLocal(i * 20).put(hello.getBytes());
			assertTrue(Arrays.equals(mappedPage.getLocal(i * 20 , length), hello.getBytes()));
		}
		
		buffer = ByteBuffer.allocateDirect(16);
		buffer.putInt(1);
		buffer.putInt(2);
		buffer.putLong(3L);
		for(int i = 0; i < 10000; i++) {
			buffer.flip();
			mappedPage.getLocal(i * 20).put(buffer);
		}
		for(int i = 0; i < 10000; i++) {
			ByteBuffer buf = mappedPage.getLocal(i * 20);
			assertTrue(1 == buf.getInt());
			assertTrue(2 == buf.getInt());
			assertTrue(3L == buf.getLong());
		}
	}
	
	@Test
	public void testMultiThreads() {
		int pageSize = 1024 * 1024 * 32;
		mappedPageFactory = new MappedPageFactoryImpl(pageSize, testDir + "/test_multi_threads", 2 * 1000);
		
		int threadNum = 100;
		int pageNumLimit = 50;
		
		Set<IMappedPage> pageSet = Collections.newSetFromMap(new ConcurrentHashMap<IMappedPage, Boolean>());
		List<ByteBuffer> localBufferList = Collections.synchronizedList(new ArrayList<ByteBuffer>());
		
		Worker[] workers = new Worker[threadNum];
		for(int i = 0; i < threadNum; i++) {
			workers[i] = new Worker(i, mappedPageFactory, pageNumLimit, pageSet, localBufferList);
		}
		for(int i = 0; i < threadNum; i++) {
			workers[i].start();
		}
		
		for(int i = 0; i< threadNum; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
		
		assertTrue(localBufferList.size() == threadNum * pageNumLimit);
		assertTrue(pageSet.size() == pageNumLimit);
		
		// verify thread locality
		for(int i = 0; i < localBufferList.size(); i++) {
			for(int j = i + 1; j < localBufferList.size(); j++) {
				if (localBufferList.get(i) == localBufferList.get(j)) {
					fail("thread local buffer is not thread local");
				}
			}
		}
	}
	
	private static class Worker extends Thread {
		private int id;
		private int pageNumLimit;
		private IMappedPageFactory pageFactory;
		private Set<IMappedPage> sharedPageSet;
		private List<ByteBuffer> localBufferList;
		
		public Worker(int id, IMappedPageFactory pageFactory, int pageNumLimit, 
				Set<IMappedPage> sharedPageSet, List<ByteBuffer> localBufferList) {
			this.id = id;
			this.pageFactory = pageFactory;
			this.sharedPageSet = sharedPageSet;
			this.localBufferList = localBufferList;
			this.pageNumLimit = pageNumLimit;
			
		}
		
		public void run() {
			for(int i = 0; i < pageNumLimit; i++) {
				try {
					IMappedPage page = this.pageFactory.acquirePage(i);
					sharedPageSet.add(page);
					localBufferList.add(page.getLocal(0));
					
					int startPosition = this.id * 2048;
					
					for(int j = 0; j < 100; j++) {
						String helloj = "hello world " + j;
						int length = helloj.getBytes().length;
						page.getLocal(startPosition + j * 20).put(helloj.getBytes());
						assertTrue(Arrays.equals(page.getLocal(startPosition + j * 20 , length), helloj.getBytes()));
					}
					
					ByteBuffer buffer = ByteBuffer.allocateDirect(16);
					buffer.putInt(1);
					buffer.putInt(2);
					buffer.putLong(3L);
					for(int j = 0; j < 100; j++) {
						buffer.flip();
						page.getLocal(startPosition + j * 20).put(buffer);
					}
					for(int j = 0; j < 100; j++) {
						ByteBuffer buf = page.getLocal(startPosition + j * 20);
						assertTrue(1 == buf.getInt());
						assertTrue(2 == buf.getInt());
						assertTrue(3L == buf.getLong());
					}
					
				} catch (IOException e) {
					fail("Got IOException when acquiring page " + i);
				}
			}
		}
		
	}
	
	@After
	public void clear() throws IOException {
		if (this.mappedPageFactory != null) {
			this.mappedPageFactory.deleteAllPages();
		}
		FileUtil.deleteDirectory(new File(testDir));
	}

}
