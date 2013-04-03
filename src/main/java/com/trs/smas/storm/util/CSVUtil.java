package com.trs.smas.storm.util;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

public class CSVUtil {
	
	private static final CSVParser parser = new CSVParser();
	
	public static String [] parse(String line) throws IOException{
		return parser.parseLine(line);
	}
}
