package com.leansoft.bigqueue.server;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import com.leansoft.bigqueue.thrift.BigQueueService;
import com.leansoft.bigqueue.thrift.QueueRequest;
import com.leansoft.bigqueue.thrift.QueueResponse;

/**
 * Big queue service thrift implementation
 * 
 * @author bulldog
 *
 */
public class BigQueueServiceImpl implements BigQueueService.Iface {
	
	private IBigQueue bigQueue;
	
	public BigQueueServiceImpl(String queueDir, String queueName) throws IOException {
		bigQueue = new BigQueueImpl(queueDir, queueName);
	}

	@Override
	public void enqueue(QueueRequest req) throws TException {
		if (req.data != null && req.data.limit() > 0) {
			try {
				bigQueue.enqueue(req.data.array());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public QueueResponse dequeue() throws TException {
		byte[] data = null;
		try {
			data = bigQueue.dequeue();
		} catch (IOException e) {
			e.printStackTrace();
		}
		QueueResponse resp = new QueueResponse();
		if (data != null && data.length > 0) {
			resp.data = ByteBuffer.wrap(data);
		}
		return resp;
	}

	@Override
	public QueueResponse peek() throws TException {
		byte[] data = null;
		try {
			data = bigQueue.peek();
		} catch (IOException e) {
			e.printStackTrace();
		}
		QueueResponse resp = new QueueResponse();
		if (data != null && data.length > 0) {
			resp.data = ByteBuffer.wrap(data);
		}
		return resp;
	}

	@Override
	public long size() throws TException {
		return this.bigQueue.size();
	}

	@Override
	public boolean isEmpty() throws TException {
		return this.bigQueue.isEmpty();
	}

}
