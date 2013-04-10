package com.trs.smas.storm.util;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

public class StringUtil {

	private static CSVParser parser = new CSVParser();
	
	public static String avoidNull(String str){
		return str==null?"":str;
	}
	
	public static String [] parse(String line){
		try {
			return parser.parseLine(line);
		} catch (IOException e) {
			return line.substring(1, line.length()-2).split("\",\"");
		}
	}
	
	public static String join(String [] ary){
		StringBuilder sb = new StringBuilder();
		for(String item : ary){
			sb.append(item).append("\n");
		}
		return sb.toString();
	}
}
