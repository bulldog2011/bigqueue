package com.leansoft.thriftqueue.load;

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

public class TestClient implements BigQueueService.Iface  {
	
	public static final String SERVER_IP = "localhost";
	public static final int SERVER_PORT = 9000;
	public static final int TIMEOUT = 30000;
	public static final String TOPIC = "log"; // same as queue name
	
	private TTransport transport = null;
	private BigQueueService.Client innerClient = null;
	
	public TestClient() throws TTransportException {
		transport = new TFramedTransport(new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT));
		TProtocol protocol = new TBinaryProtocol(transport);
		innerClient = new BigQueueService.Client(protocol);
		transport.open();
	}
	
	public void close() {
		if (transport != null) {
			transport.close();
		}
	}

	@Override
	public QueueResponse enqueue(String topic, QueueRequest req)
			throws TException {
		return this.innerClient.enqueue(topic, req);
	}

	@Override
	public QueueResponse dequeue(String topic) throws TException {
		return this.innerClient.dequeue(topic);
	}

	@Override
	public QueueResponse peek(String topic) throws TException {
		return this.innerClient.peek(topic);
	}

	@Override
	public long getSize(String topic) throws TException {
		return this.innerClient.getSize(topic);
	}

	@Override
	public boolean isEmpty(String topic) throws TException {
		return this.innerClient.isEmpty(topic);
	}

}
