package com.leansoft.bigqueue.utils;

import static org.junit.Assert.*;

import org.junit.Test;

import com.leansoft.bigqueue.utils.Calculator;

public class CalculatorTest {

	@Test
	public void testCalculator() {
		// test mod
		assertTrue(Calculator.mod(1024, 1) == 0);
		assertTrue(Calculator.mod(1024, 2) == 0);
		assertTrue(Calculator.mod(1024, 5) == 0);
		assertTrue(Calculator.mod(1024, 10) == 0);
		
		assertTrue(Calculator.mod(1025, 10) == 1);
		assertTrue(Calculator.mod(1027, 10) == 3);
		
		assertTrue(Calculator.mod(0, 10) == 0);
		
		assertTrue(Calculator.mod(Long.MAX_VALUE, 10) == 1023);
		for(int i = 0; i <= 1023; i++) {
			assertTrue(Calculator.mod(Long.MAX_VALUE - i, 10) == 1023 - i);
		}
		
		// test mul
		for(int i = 0; i <= 60; i++) {
			assertTrue(Calculator.mul(Integer.MAX_VALUE, i) == Integer.MAX_VALUE * (long)Math.pow(2, i));
		}
		
		// test div
		for(int i = 0; i <= 60; i++) {
			assertTrue(Calculator.div(Long.MAX_VALUE, i) == Long.MAX_VALUE / (long)Math.pow(2, i));
		}
	}

}
