package com.leansoft.thriftqueue.load.helper;

import java.util.Random;

public class RandomStringUtil {
	
	private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ ";
	private static Random rnd = new Random();

	public static String genRandomString(int len ) 
	{
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}

}
