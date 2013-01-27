package com.leansoft.thriftqueue.load.helper;

import java.util.Random;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.leansoft.logging.thrift.LogEvent;
import com.leansoft.logging.thrift.LogLevel;

public class RandomLogGenerator {
	
	static Random random = new Random();
	static ThreadLocalTSerializer tSerializer = new ThreadLocalTSerializer();
	static ThreadLocalTDeserializer tDeserializer = new ThreadLocalTDeserializer();
	
	public static LogEvent genRandomLogEvent(int maxMessageSize) {
		LogEvent logEvent = new LogEvent();
		logEvent.createdTime = System.currentTimeMillis();
		logEvent.hostId = "localhost";
		int value = random.nextInt(5);
		logEvent.logLevel = LogLevel.findByValue(value);
		int size = random.nextInt(maxMessageSize) + 1;
		if (size < 10) {
			size += 10;
		}
		logEvent.message = RandomStringUtil.genRandomString(size);
		return logEvent;
	}
	
	public static byte[] genRandomLogEventBytes(int maxMessageSize) throws TException {
		LogEvent logEvent = genRandomLogEvent(maxMessageSize);
		TSerializer localSerializer = tSerializer.get();
		return localSerializer.serialize(logEvent);
	}
	
	public LogEvent getLogEventFromBytes(byte[] data) throws TException {
		LogEvent logEvent = new LogEvent();
		TDeserializer localDeserializer = tDeserializer.get();
		localDeserializer.deserialize(logEvent, data);
		return logEvent;
	}
	
	static class ThreadLocalTSerializer extends ThreadLocal<TSerializer> {
    	@Override
    	protected synchronized TSerializer initialValue() {
    		return new TSerializer();
    	}
	}
	
	static class ThreadLocalTDeserializer extends ThreadLocal<TDeserializer> {
    	@Override
    	protected synchronized TDeserializer initialValue() {
    		return new TDeserializer();
    	}
	}


}
