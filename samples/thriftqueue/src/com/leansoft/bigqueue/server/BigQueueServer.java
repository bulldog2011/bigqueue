package com.leansoft.bigqueue.server;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import com.leansoft.bigqueue.thrift.BigQueueService;

/**
 * Big queue server based on Thrift
 * 
 * @author bulldog
 *
 */
public class BigQueueServer {
	
	public static final int SERVER_PORT = 9000;
	// adjust the queue dir and name according to your environment and requirement
	public static final String QUEUE_DIR = "/bigqueue/server/";
	public static final String QUEUE_NAME = "thrift-queue";
	
	public void start() {
		try {
			System.out.println("Big Queue server start ...");
			
			BigQueueService.Iface bigQueueSerivce = new BigQueueServiceImpl(QUEUE_DIR, QUEUE_NAME);
			TProcessor tprocessor = new BigQueueService.Processor(bigQueueSerivce);
			
			TNonblockingServerSocket tnbSocketTransport = new TNonblockingServerSocket(SERVER_PORT);
			TNonblockingServer.Args tnbArgs = new TNonblockingServer.Args(tnbSocketTransport);
			tnbArgs.processor(tprocessor);
			// Nonblocking server mode needs TFramedTransport
			tnbArgs.transportFactory(new TFramedTransport.Factory());
			tnbArgs.protocolFactory(new TCompactProtocol.Factory());
			
			TServer server = new TNonblockingServer(tnbArgs);
			System.out.println("Big Queue TNonblockingServer started on port " + SERVER_PORT);
			server.serve();
		} catch (Exception e) {
			System.err.println("Server start error!!!");
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		BigQueueServer server = new BigQueueServer();
		server.start();
	}

}
