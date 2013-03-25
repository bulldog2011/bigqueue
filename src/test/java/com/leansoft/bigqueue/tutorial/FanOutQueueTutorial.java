package com.leansoft.bigqueue.tutorial;

import java.io.IOException;

import org.junit.Test;

import com.leansoft.bigqueue.FanOutQueueImpl;
import com.leansoft.bigqueue.IFanOutQueue;

/**
 * A tutorial to show the basic API usage of the fanout queue.
 * 
 * @author bulldog
 *
 */
public class FanOutQueueTutorial {

	@Test
	public void demo() throws IOException {
		IFanOutQueue foQueue = null;
		
		try {
			// create a new fanout queue
			foQueue = new FanOutQueueImpl("d:/tutorial/fanout-queue", "demo");
			
			// enqueue some logs
			for(int i = 0; i < 10; i++) {
				String log = "log-" + i;
				foQueue.enqueue(log.getBytes());
			}
			
			// consuming the queue with fanoutId 1
			String fanoutId1 = "realtime";
			System.out.println("output from " + fanoutId1 + " consumer:");
			while(!foQueue.isEmpty(fanoutId1)) {
				String item = new String(foQueue.dequeue(fanoutId1));
				System.out.println(item);
			}
			
			// consuming the queue with fanoutId 2
			String fanoutId2 = "offline";
			System.out.println("output from " + fanoutId2 + " consumer:");
			while(!foQueue.isEmpty(fanoutId2)) {
				String item = new String(foQueue.dequeue(fanoutId2));
				System.out.println(item);
			}
		} finally {
			// release resource
			if (foQueue != null) {
				foQueue.close();
			}
		}
	}

}
