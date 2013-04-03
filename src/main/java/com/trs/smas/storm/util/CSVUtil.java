package com.trs.smas.storm.util;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

public class CSVUtil {
	
	private static final CSVParser parser = new CSVParser();
	
	public static String [] parse(String line){
		try {
			return parser.parseLine(line);
		} catch (IOException e) {
			return new String [0];
		}
	}
}
