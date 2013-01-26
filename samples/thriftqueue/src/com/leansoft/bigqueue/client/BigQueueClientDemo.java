package com.leansoft.bigqueue.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.leansoft.bigqueue.thrift.BigQueueService;
import com.leansoft.bigqueue.thrift.QueueRequest;
import com.leansoft.bigqueue.thrift.QueueResponse;

/**
 * A sample big queue client which communicates with big queue server through Thrift RPC.
 * 
 * @author bulldog
 *
 */
public class BigQueueClientDemo {
	
	public static final String SERVER_IP = "localhost";
	public static final int SERVER_PORT = 9000;
	public static final int TIMEOUT = 30000;
	
	private TTransport transport = null;
	private BigQueueService.Client client = null;
	
	private void initClient() throws TTransportException {
		transport = new TFramedTransport(new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT));
		TProtocol protocol = new TCompactProtocol(transport);
		client = new BigQueueService.Client(protocol);
		transport.open();
	}
	
	private void closeClient() {
		if (transport != null) {
			transport.close();
		}
	}
	
	public void run() throws TException {
		
		try {
			this.initClient();
			
			System.out.println("big queue size before enqueue : " + client.size());
			
			QueueRequest req = new QueueRequest();
			req.setData("hello world".getBytes());
			client.enqueue(req);
			
			System.out.println("big queue size after enqueue : " + client.size());
			
			QueueResponse resp = client.peek();
			System.out.println("big queue size after peek : " + client.size());
			System.out.println("peeked message : " + new String(resp.getData()));
			
			resp = client.dequeue();
			System.out.println("big queue size after dequeue : " + client.size());
			System.out.println("dequeued message : " + new String(resp.getData()));
		} finally {
			this.closeClient();
		}
		
	}
	
	public static void main(String[] args){
		BigQueueClientDemo clientDemo = new BigQueueClientDemo();
		try {
			clientDemo.run();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
