package com.leansoft.thriftqueue.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
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
public class ThriftQueueClientDemo {
	
	public static final String SERVER_IP = "localhost";
	public static final int SERVER_PORT = 9000;
	public static final int TIMEOUT = 30000;
	public static final String TOPIC = "log"; // same as queue name
	
	private TTransport transport = null;
	private BigQueueService.Client client = null;
	
	private void initClient() throws TTransportException {
		transport = new TFramedTransport(new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT));
		TProtocol protocol = new TBinaryProtocol(transport);
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
			
			System.out.println("big queue size before enqueue : " + client.getSize(TOPIC));
			
			QueueRequest req = new QueueRequest();
			req.setData("hello world".getBytes());
			client.enqueue(TOPIC, req);
			
			System.out.println("big queue size after enqueue : " + client.getSize(TOPIC));
			
			QueueResponse resp = client.peek(TOPIC);
			System.out.println("big queue size after peek : " + client.getSize(TOPIC));
			System.out.println("peeked message : " + new String(resp.getData()));
			
			resp = client.dequeue(TOPIC);
			System.out.println("big queue size after dequeue : " + client.getSize(TOPIC));
			System.out.println("dequeued message : " + new String(resp.getData()));
		} finally {
			this.closeClient();
		}
		
	}
	
	public static void main(String[] args){
		ThriftQueueClientDemo clientDemo = new ThriftQueueClientDemo();
		try {
			clientDemo.run();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
