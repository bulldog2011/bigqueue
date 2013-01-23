package com.leansoft.bigqueue;

import java.util.Random;

public class TestUtil {
	
	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random();

	public static String randomString(int len ) 
	{
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}
	
	public static void sleepQuietly(long duration) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			// ignore
		}
	}
	
	public static final String TEST_BASE_DIR = "e:/bigq_test/";
}
